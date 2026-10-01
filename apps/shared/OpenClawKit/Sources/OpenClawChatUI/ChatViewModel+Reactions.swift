import Foundation
import Observation

@MainActor
@Observable
final class ChatMessageReactionState {
    struct Target: Equatable {
        let session: OpenClawChatViewModel.SessionSnapshot
        let sessionID: String
    }

    var summaries: [String: [OpenClawChatReactionSummary]] = [:]
    var errors: [String: String] = [:]
    var writes: [String: UUID] = [:]
    var lease: OpenClawChatReactionsRouteLease?
    var target: Target?
    @ObservationIgnored var revisions: [String: UInt64] = [:]
    @ObservationIgnored var readUpdates: [String: [OpenClawChatReactionSummary]]?
    var refreshID = UUID()
    @ObservationIgnored var refreshTask: Task<Void, Never>?

    func reset() {
        self.refreshID = UUID()
        self.refreshTask?.cancel()
        self.refreshTask = nil
        self.target = nil
        self.lease = nil
        self.summaries = [:]
        self.errors = [:]
        self.writes = [:]
        self.revisions = [:]
        self.readUpdates = nil
    }
}

extension OpenClawChatViewModel {
    public var reactionContextID: UUID {
        self.reactionState.refreshID
    }

    public var viewerReactionUserID: String? {
        self.reactionState.lease?.access.userID
    }

    public func messageReactions(for message: OpenClawChatMessage) -> [OpenClawChatReactionSummary] {
        guard let target = self.reactionState.target, self.isCurrentReactionTarget(target),
              let messageID = self.savedReactionMessageID(message)
        else { return [] }
        return self.reactionState.summaries[messageID] ?? []
    }

    public func isReactionPending(for message: OpenClawChatMessage) -> Bool {
        message.transcriptMessageID.map { self.reactionState.writes[$0] != nil } ?? false
    }

    public func reactionError(for message: OpenClawChatMessage) -> String? {
        message.transcriptMessageID.flatMap { self.reactionState.errors[$0] }
    }

    public func canReact(to message: OpenClawChatMessage) -> Bool {
        guard self.savedReactionMessageID(message) != nil,
              self.hasCurrentSessionMetadata,
              let target = self.reactionState.target,
              self.isCurrentReactionTarget(target),
              let access = self.reactionState.lease?.access,
              let session = self.currentSessionEntry()
        else { return false }
        return access.canReact(
            sharingRole: session.sharingRole?.rawValue,
            visibility: session.visibility?.rawValue,
            archived: session.isArchived,
            catalog: Self.isReactionCatalogSession(self.sessionKey))
    }

    public func toggleMessageReaction(message: OpenClawChatMessage, emoji: String) async {
        guard OpenClawChatReactionEmoji.isValid(emoji),
              self.canReact(to: message),
              let messageID = self.savedReactionMessageID(message),
              self.reactionState.writes[messageID] == nil,
              let target = self.reactionState.target,
              let lease = self.reactionState.lease
        else { return }
        let operation = UUID()
        self.reactionState.writes[messageID] = operation
        self.reactionState.errors[messageID] = nil
        defer {
            if self.reactionState.writes[messageID] == operation {
                self.reactionState.writes[messageID] = nil
            }
        }
        guard await lease.isCurrent(),
              self.isCurrentReactionWrite(target, routeID: lease.routeID, messageID: messageID, operation: operation),
              self.canReact(to: message)
        else { return }
        let revision = self.reactionState.revisions[messageID, default: 0]
        let remove = self.messageReactions(for: message).contains { summary in
            summary.emoji == emoji && summary.identities.contains { $0.id == lease.access.userID }
        }
        do {
            let result = try await lease.set(
                sessionKey: target.session.key,
                agentID: target.session.deliveryAgentID,
                messageID: messageID,
                emoji: emoji,
                remove: remove)
            guard await lease.isCurrent(),
                  self.isCurrentReactionWrite(
                      target, routeID: lease.routeID, messageID: messageID, operation: operation),
                  self.savedReactionMessageID(message) != nil,
                  result.messageID == messageID,
                  self.reactionState.revisions[messageID, default: 0] == revision
            else { return }
            self.reactionState.summaries[messageID] = result.reactions
            self.reactionState.readUpdates?[messageID] = result.reactions
        } catch {
            guard await lease.isCurrent(),
                  self.isCurrentReactionWrite(
                      target, routeID: lease.routeID, messageID: messageID, operation: operation),
                  self.savedReactionMessageID(message) != nil
            else { return }
            self.reactionState.errors[messageID] = error.localizedDescription
        }
    }

    func resetSessionReactions() {
        self.reactionState.reset()
    }

    func syncSessionReactions(refreshMetadata: Bool = false) {
        guard !self.usesWebConversation, self.healthOK, !self.isTransportDetached,
              self.hasAppliedLiveHistory,
              !Self.isReactionCatalogSession(self.sessionKey),
              let sessionID = self.reactionSessionID
        else {
            self.resetSessionReactions()
            return
        }
        let target = ChatMessageReactionState.Target(session: self.currentSessionSnapshot(), sessionID: sessionID)
        guard self.historyMatchesReactionTarget(target) else {
            self.resetSessionReactions()
            return
        }
        guard self.reactionState.target != target else { return }
        self.resetSessionReactions()
        self.reactionState.target = target
        self.reactionState.readUpdates = [:]
        let refreshID = self.reactionState.refreshID
        self.reactionState.refreshTask = Task { [weak self] in
            await self?.loadSessionReactions(target, refreshID: refreshID, refreshMetadata: refreshMetadata)
        }
    }

    func handleSessionReactionEvent(_ event: OpenClawChatReactionEvent) {
        guard self.healthOK,
              self.currentSessionSnapshot().deliveryAgentID.map({
                  $0 == event.agentID.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
              }) ?? true,
              self.matchesCurrentSessionKey(
                  incoming: event.sessionKey, agentId: event.agentID, current: self.sessionKey),
              event.sessionID == self.reactionSessionID,
              !Self.isReactionCatalogSession(self.sessionKey)
        else { return }
        self.syncSessionReactions()
        guard let target = self.reactionState.target, self.isCurrentReactionTarget(target) else { return }
        self.reactionState.revisions[event.messageID, default: 0] &+= 1
        self.reactionState.readUpdates?[event.messageID] = event.reactions
        self.reactionState.summaries[event.messageID] = event.reactions
    }

    private var reactionSessionID: String? {
        let sessionID = self.hasCurrentSessionMetadata
            ? self.currentSessionEntry()?.sessionId ?? self.sessionId : self.sessionId
        return ChatPayloadDecoding.trimmedNonEmptyString(sessionID)
    }

    private func savedReactionMessageID(_ message: OpenClawChatMessage) -> String? {
        let role = message.role.lowercased()
        guard role == "user" || role == "assistant",
              let messageID = ChatPayloadDecoding.trimmedNonEmptyString(message.transcriptMessageID),
              self.messages.contains(where: { $0.transcriptMessageID == messageID && $0.role.lowercased() == role })
        else { return nil }
        return messageID
    }

    private func isCurrentReactionTarget(_ target: ChatMessageReactionState.Target) -> Bool {
        self.healthOK && self.isCurrentSession(target.session) &&
            self.currentSessionSnapshot().deliveryAgentID == target.session.deliveryAgentID &&
            self.reactionState.target == target && self.reactionSessionID == target.sessionID &&
            self.historyMatchesReactionTarget(target)
    }

    private func historyMatchesReactionTarget(_ target: ChatMessageReactionState.Target) -> Bool {
        self.sessionId.map { $0 == target.sessionID } ?? true
    }

    private func isCurrentReactionWrite(
        _ target: ChatMessageReactionState.Target,
        routeID: UUID,
        messageID: String,
        operation: UUID) -> Bool
    {
        self.isCurrentReactionTarget(target) && self.reactionState.lease?.routeID == routeID &&
            self.reactionState.writes[messageID] == operation
    }

    private func loadSessionReactions(
        _ target: ChatMessageReactionState.Target,
        refreshID: UUID,
        refreshMetadata: Bool) async
    {
        defer {
            if self.reactionState.refreshID == refreshID {
                self.reactionState.readUpdates = nil
                self.reactionState.refreshTask = nil
            }
        }
        guard let lease = await self.transport.acquireReactionsRouteLease(),
              await lease.isCurrent(),
              self.reactionState.refreshID == refreshID,
              self.isCurrentReactionTarget(target)
        else { return }
        self.reactionState.lease = lease
        if refreshMetadata, !self.hasCurrentSessionMetadata {
            await self.fetchSessions(limit: Self.sessionListFetchLimit, sessionSnapshot: target.session)
        }
        guard self.isCurrentReactionTarget(target), lease.access.canList else { return }
        do {
            let result = try await lease.list(sessionKey: target.session.key, agentID: target.session.deliveryAgentID)
            guard await lease.isCurrent(),
                  self.reactionState.refreshID == refreshID,
                  self.isCurrentReactionTarget(target),
                  self.reactionState.lease?.routeID == lease.routeID,
                  result.sessionID == target.sessionID
            else { return }
            // Events committed after the read began outrank its older snapshot.
            self.reactionState.summaries = result.reactions.merging(self.reactionState.readUpdates ?? [:]) { _, new in
                new
            }
        } catch {
            guard await lease.isCurrent(),
                  self.reactionState.refreshID == refreshID,
                  self.isCurrentReactionTarget(target)
            else { return }
            self.errorText = error.localizedDescription
        }
    }

    private static func isReactionCatalogSession(_ key: String) -> Bool {
        var source = key
        if source.hasPrefix("agent:"), let separator = source.dropFirst(6).firstIndex(of: ":"),
           separator != source.index(source.startIndex, offsetBy: 6)
        {
            source = String(source[source.index(after: separator)...])
        }
        guard source.hasPrefix("catalog:") else { return false }
        let parts = source.dropFirst(8).split(separator: ":", omittingEmptySubsequences: false)
        return parts.count == 3 && parts.allSatisfy { part in
            guard let decoded = String(part).removingPercentEncoding else { return false }
            return !decoded.isEmpty
        }
    }
}
