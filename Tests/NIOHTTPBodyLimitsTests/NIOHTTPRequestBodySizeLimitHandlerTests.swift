//===----------------------------------------------------------------------===//
//
// This source file is part of the SwiftNIO open source project
//
// Copyright (c) 2026 Apple Inc. and the SwiftNIO project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of SwiftNIO project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import HTTPTypes
import NIOCore
import NIOEmbedded
import NIOHTTPTypes
import XCTest

@testable import NIOHTTPBodyLimits

/// Collects inbound request parts so tests can assert what was forwarded past the limit handler.
final class BodyCollector: ChannelInboundHandler {
    typealias InboundIn = HTTPRequestPart

    var receivedBytes = 0
    var ended = false

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        switch self.unwrapInboundIn(data) {
        case .head:
            break
        case .body(let buffer):
            self.receivedBytes += buffer.readableBytes
        case .end:
            self.ended = true
        }
        context.fireChannelRead(data)
    }
}

/// Captures errors fired by the handler (inbound errors travel towards the head of the pipeline).
final class ErrorCollector: ChannelInboundHandler {
    typealias InboundIn = HTTPRequestPart

    var error: Error?

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        self.error = error
    }
}

final class NIOHTTPRequestBodySizeLimitHandlerTests: XCTestCase {
    private func makeRequest(
        method: HTTPRequest.Method = .post,
        path: String = "/upload",
        contentLength: String? = nil
    ) -> HTTPRequest {
        var request = HTTPRequest(
            method: method,
            scheme: "http",
            authority: "localhost:8888",
            path: path
        )
        if let contentLength {
            request.headerFields[.contentLength] = contentLength
        }
        return request
    }

    /// Pipeline: ErrorCollector (head side) -> limit handler -> BodyCollector (tail side).
    private func makeChannel(
        limit: Int,
        overflowAction: NIOHTTPBodyLimitOverflowAction = .respond(status: .contentTooLarge)
    ) throws -> (EmbeddedChannel, ErrorCollector, BodyCollector) {
        let channel = EmbeddedChannel()
        let errorCollector = ErrorCollector()
        let bodyCollector = BodyCollector()
        try channel.pipeline.syncOperations.addHandler(errorCollector)
        try channel.pipeline.syncOperations.addHandler(
            NIOHTTPRequestBodySizeLimitHandler(limit: limit, overflowAction: overflowAction)
        )
        try channel.pipeline.syncOperations.addHandler(bodyCollector)
        return (channel, errorCollector, bodyCollector)
    }

    private func readAllOutboundResponses(_ channel: EmbeddedChannel) throws -> [HTTPResponsePart] {
        var parts: [HTTPResponsePart] = []
        while let part: HTTPResponsePart? = try channel.readOutbound() {
            parts.append(part!)
        }
        return parts
    }

    // MARK: - Happy paths

    func testBodyWithinLimitIsForwarded() throws {
        let (channel, errorCollector, bodyCollector) = try makeChannel(limit: 10)
        try channel.writeInbound(HTTPRequestPart.head(makeRequest(contentLength: "10")))
        try channel.writeInbound(HTTPRequestPart.body(ByteBuffer(string: "Hello")))
        try channel.writeInbound(HTTPRequestPart.body(ByteBuffer(string: "Hello")))
        try channel.writeInbound(HTTPRequestPart.end(nil))

        XCTAssertEqual(bodyCollector.receivedBytes, 10)
        XCTAssertTrue(bodyCollector.ended)
        XCTAssertNil(errorCollector.error)
        XCTAssertNil(try channel.readOutbound(as: HTTPResponsePart.self))
    }

    func testBodyExactlyAtLimitIsForwarded() throws {
        let (channel, errorCollector, bodyCollector) = try makeChannel(limit: 10)
        try channel.writeInbound(HTTPRequestPart.head(makeRequest()))
        try channel.writeInbound(HTTPRequestPart.body(ByteBuffer(string: "0123456789")))
        try channel.writeInbound(HTTPRequestPart.end(nil))

        XCTAssertEqual(bodyCollector.receivedBytes, 10)
        XCTAssertTrue(bodyCollector.ended)
        XCTAssertNil(errorCollector.error)
    }

    func testEmptyRequestBodyIsForwarded() throws {
        let (channel, _, bodyCollector) = try makeChannel(limit: 10)
        try channel.writeInbound(HTTPRequestPart.head(makeRequest(method: .get)))
        try channel.writeInbound(HTTPRequestPart.end(nil))

        XCTAssertEqual(bodyCollector.receivedBytes, 0)
        XCTAssertTrue(bodyCollector.ended)
    }

    func testZeroLimitRejectsAnyBody() throws {
        let (channel, errorCollector, bodyCollector) = try makeChannel(limit: 0)
        try channel.writeInbound(HTTPRequestPart.head(makeRequest(contentLength: "0")))
        try channel.writeInbound(HTTPRequestPart.end(nil))

        XCTAssertEqual(bodyCollector.receivedBytes, 0)
        XCTAssertTrue(bodyCollector.ended)
        XCTAssertNil(errorCollector.error)

        // Any body on the next (pipelined) request is rejected.
        try channel.writeInbound(HTTPRequestPart.head(makeRequest(contentLength: "1")))
        let responses = try readAllOutboundResponses(channel)
        XCTAssertEqual(responses.count, 2)
        guard let first = responses.first, case let .head(head) = first else {
            return XCTFail("expected response head")
        }
        XCTAssertEqual(head.status, .contentTooLarge)
        XCTAssertFalse(channel.isActive)
    }

    // MARK: - Header layer violations

    func testDeclaredContentLengthExceedingLimitRejectsImmediately() throws {
        let (channel, errorCollector, bodyCollector) = try makeChannel(limit: 10)
        try channel.writeInbound(HTTPRequestPart.head(makeRequest(contentLength: "11")))

        // The reject response must be written before any body bytes arrive.
        let responses = try readAllOutboundResponses(channel)
        XCTAssertEqual(responses.count, 2)
        guard let first = responses.first, case let .head(head) = first else {
            return XCTFail("expected response head")
        }
        XCTAssertEqual(head.status, .contentTooLarge)
        XCTAssertEqual(head.headerFields[.connection], "close")

        // Nothing from the offending request is forwarded to the application.
        XCTAssertEqual(bodyCollector.receivedBytes, 0)
        XCTAssertNil(errorCollector.error)
        XCTAssertFalse(channel.isActive)
    }

    func testMalformedContentLengthFallsBackToStreamingCheck() throws {
        let (channel, _, bodyCollector) = try makeChannel(limit: 10)
        try channel.writeInbound(HTTPRequestPart.head(makeRequest(contentLength: "not-a-number")))
        try channel.writeInbound(HTTPRequestPart.body(ByteBuffer(string: "0123456789")))
        try channel.writeInbound(HTTPRequestPart.end(nil))

        XCTAssertEqual(bodyCollector.receivedBytes, 10)
        XCTAssertTrue(bodyCollector.ended)
    }

    // MARK: - Streaming layer violations

    func testStreamedBodyExceedingLimitIsRejected() throws {
        let (channel, errorCollector, bodyCollector) = try makeChannel(limit: 10)

        try channel.writeInbound(HTTPRequestPart.head(makeRequest()))
        try channel.writeInbound(HTTPRequestPart.body(ByteBuffer(string: "Hello")))
        try channel.writeInbound(HTTPRequestPart.body(ByteBuffer(string: "Hello")))
        XCTAssertEqual(bodyCollector.receivedBytes, 10)
        XCTAssertNil(errorCollector.error)

        // The 11th byte trips the limit: this frame must not reach the application.
        try channel.writeInbound(HTTPRequestPart.body(ByteBuffer(string: "!")))

        let responses = try readAllOutboundResponses(channel)
        XCTAssertEqual(responses.count, 2)
        guard let first = responses.first, case let .head(head) = first else {
            return XCTFail("expected response head")
        }
        XCTAssertEqual(head.status, .contentTooLarge)
        XCTAssertEqual(head.headerFields[.contentLength], "0")

        // Only the two in-limit frames were forwarded; the end of the rejected request was not.
        XCTAssertEqual(bodyCollector.receivedBytes, 10)
        XCTAssertFalse(bodyCollector.ended)
        XCTAssertFalse(channel.isActive)
    }

    func testUndersizedChunkedBodyIsRejectedMidStream() throws {
        // A declared content-length that is too small does not protect the client:
        // actual bytes are still counted while streaming. The header check passes,
        // the streaming check trips one frame later.
        let (channel, _, bodyCollector) = try makeChannel(limit: 10)
        try channel.writeInbound(HTTPRequestPart.head(makeRequest(contentLength: "4")))
        try channel.writeInbound(HTTPRequestPart.body(ByteBuffer(string: "Hello")))
        try channel.writeInbound(HTTPRequestPart.body(ByteBuffer(string: "Hello")))
        XCTAssertEqual(bodyCollector.receivedBytes, 10)
        try channel.writeInbound(HTTPRequestPart.body(ByteBuffer(string: "!")))

        let responses = try readAllOutboundResponses(channel)
        XCTAssertEqual(responses.count, 2)
        guard let first = responses.first, case let .head(head) = first else {
            return XCTFail("expected response head")
        }
        XCTAssertEqual(head.status, .contentTooLarge)
        XCTAssertEqual(bodyCollector.receivedBytes, 10)
    }

    // MARK: - `.fail` action

    func testFailActionFiresErrorAndLeavesResponseToApplication() throws {
        let (channel, errorCollector, bodyCollector) = try makeChannel(
            limit: 10,
            overflowAction: .fail
        )
        try channel.writeInbound(HTTPRequestPart.head(makeRequest(path: "/big-upload")))
        try channel.writeInbound(HTTPRequestPart.body(ByteBuffer(string: "0123456789A")))

        // No HTTP response is written by the handler itself.
        XCTAssertNil(try channel.readOutbound(as: HTTPResponsePart.self))

        guard let error = errorCollector.error as? NIOHTTPBodyLimitOverflowError else {
            XCTFail("expected NIOHTTPBodyLimitOverflowError, got \(String(describing: errorCollector.error))")
            return
        }
        XCTAssertEqual(error.method, .post)
        XCTAssertEqual(error.path, "/big-upload")
        XCTAssertEqual(error.limit, 10)
        XCTAssertEqual(error.received, 11)

        XCTAssertEqual(bodyCollector.receivedBytes, 0)
        XCTAssertFalse(channel.isActive)
    }

    func testFailActionHeaderViolationCarriesDeclaredLength() throws {
        let (channel, errorCollector, _) = try makeChannel(limit: 10, overflowAction: .fail)
        try channel.writeInbound(HTTPRequestPart.head(makeRequest(contentLength: "1024")))

        guard let error = errorCollector.error as? NIOHTTPBodyLimitOverflowError else {
            XCTFail("expected NIOHTTPBodyLimitOverflowError, got \(String(describing: errorCollector.error))")
            return
        }
        XCTAssertEqual(error.received, 1024)
        XCTAssertEqual(error.limit, 10)
    }

    // MARK: - Pipelining

    func testPipelinedRequestsAreHandledIndependently() throws {
        let (channel, errorCollector, bodyCollector) = try makeChannel(limit: 10)

        // First request: within limit, fully forwarded.
        try channel.writeInbound(HTTPRequestPart.head(makeRequest(path: "/first")))
        try channel.writeInbound(HTTPRequestPart.body(ByteBuffer(string: "Hello")))
        try channel.writeInbound(HTTPRequestPart.end(nil))
        XCTAssertEqual(bodyCollector.receivedBytes, 5)
        XCTAssertNil(errorCollector.error)

        // Second request: violates the limit; the connection is then closed.
        try channel.writeInbound(HTTPRequestPart.head(makeRequest(path: "/second", contentLength: "99")))
        let responses = try readAllOutboundResponses(channel)
        XCTAssertEqual(responses.count, 2)
        guard let first = responses.first, case let .head(head) = first else {
            return XCTFail("expected response head")
        }
        XCTAssertEqual(head.status, .contentTooLarge)

        // Only the first request reached the application.
        XCTAssertEqual(bodyCollector.receivedBytes, 5)
        XCTAssertFalse(channel.isActive)
    }
}
