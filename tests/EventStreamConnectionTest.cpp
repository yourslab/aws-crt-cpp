/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#include <aws/crt/Api.h>

#include <aws/crt/event-stream/EventStream.h>

#include <aws/testing/aws_test_harness.h>

#include <sstream>
#include <iostream>

using namespace Aws::Crt;

static int s_TestEventStreamConnect(struct aws_allocator *allocator, void *ctx)
{
    (void)ctx;
    {
        ApiHandle apiHandle(allocator);
        Io::TlsContextOptions tlsCtxOptions = Io::TlsContextOptions::InitDefaultClient();
        Io::TlsContext tlsContext(tlsCtxOptions, Io::TlsMode::CLIENT, allocator);
        ASSERT_TRUE(tlsContext);

        Io::TlsConnectionOptions tlsConnectionOptions = tlsContext.NewConnectionOptions();
        Io::SocketOptions socketOptions;
        socketOptions.SetConnectTimeoutMs(1000);

        Io::EventLoopGroup eventLoopGroup(0, allocator);
        ASSERT_TRUE(eventLoopGroup);

        Io::DefaultHostResolver defaultHostResolver(eventLoopGroup, 8, 30, allocator);
        ASSERT_TRUE(defaultHostResolver);

        Io::ClientBootstrap clientBootstrap(eventLoopGroup, defaultHostResolver, allocator);
        ASSERT_TRUE(clientBootstrap);
        clientBootstrap.EnableBlockingShutdown();
        Aws::Crt::List<Eventstream::EventStreamHeader> authHeaders;
        authHeaders.push_back(Eventstream::EventStreamHeader(String("client-name"), String("accepted.testy_mc_testerson"), allocator));
        Eventstream::MessageAmendment connectionAmendment(authHeaders);
        auto messageAmender = [&](void) -> Eventstream::MessageAmendment& {
            return connectionAmendment;
        };
        std::shared_ptr<Eventstream::EventstreamRpcConnection> connection(nullptr);
        bool errorOccured = true;
        bool connectionShutdown = false;

        std::condition_variable semaphore;
        std::mutex semaphoreLock;

        auto onConnect = [&](const std::shared_ptr<Eventstream::EventstreamRpcConnection> &newConnection) {
            std::lock_guard<std::mutex> lockGuard(semaphoreLock);

            std::cout << "Connected" << std::endl;

            connection = newConnection;

            semaphore.notify_one();
        };

        auto onDisconnect = [&](const std::shared_ptr<Eventstream::EventstreamRpcConnection> &newConnection,
                                int errorCode) 
        {
            std::lock_guard<std::mutex> lockGuard(semaphoreLock);

            std::cout << "Disconnected" << std::endl;

            if(errorCode) errorOccured = true; else connectionShutdown = true;

            connection = newConnection;

            semaphore.notify_one();
        };

        String hostName = "127.0.0.1";
        Eventstream::EventstreamRpcConnectionOptions options;
        options.Bootstrap = &clientBootstrap;
        options.SocketOptions = socketOptions;
        options.HostName = hostName;
        options.Port = 8033;
        options.ConnectMessageAmenderCallback = messageAmender;
        options.OnConnectCallback = onConnect;
        options.OnDisconnectCallback = onDisconnect;
        options.OnErrorCallback = nullptr;
        options.OnPingCallback = nullptr;

        {
            std::unique_lock<std::mutex> semaphoreULock(semaphoreLock);
            ASSERT_TRUE(Eventstream::EventstreamRpcConnection::CreateConnection(options, allocator));
            semaphore.wait(semaphoreULock, [&]() { return connection; });
            ASSERT_TRUE(connection);
            connection->Close();
        }
    }

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(EventStreamConnect, s_TestEventStreamConnect)
