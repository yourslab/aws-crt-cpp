/*
 * Copyright 2010-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
#include <aws/crt/http/HttpConnectionManager.h>

#include <algorithm>
#include <aws/http/connection_manager.h>

namespace Aws
{
    namespace Crt
    {
        namespace Http
        {
            struct ConnectionManagerCallbackArgs
            {
                ConnectionManagerCallbackArgs() = default;
                OnClientConnectionAvailable m_onClientConnectionAvailable;
                std::shared_ptr<HttpClientConnectionManager> m_connectionManager;
            };

            void HttpClientConnectionManager::s_shutdownCompleted(void *userData) noexcept
            {
                HttpClientConnectionManager *connectionManager =
                    reinterpret_cast<HttpClientConnectionManager *>(userData);
                connectionManager->m_shutdownPromise.set_value();
            }

            HttpClientConnectionManagerOptions::HttpClientConnectionManagerOptions() noexcept
                : ConnectionOptions(), MaxConnections(1), EnableBlockingShutdown(false)
            {
            }

            std::shared_ptr<HttpClientConnectionManager> HttpClientConnectionManager::NewClientConnectionManager(
                const HttpClientConnectionManagerOptions &connectionManagerOptions,
                Allocator *allocator) noexcept
            {
                auto *toSeat = static_cast<HttpClientConnectionManager *>(
                    aws_mem_acquire(allocator, sizeof(HttpClientConnectionManager)));
                if (toSeat)
                {
                    toSeat = new (toSeat) HttpClientConnectionManager(connectionManagerOptions, allocator);
                    return std::shared_ptr<HttpClientConnectionManager>(
                        toSeat, [allocator](HttpClientConnectionManager *manager) { Delete(manager, allocator); });
                }

                return nullptr;
            }

            HttpClientConnectionManager::HttpClientConnectionManager(
                const HttpClientConnectionManagerOptions &options,
                Allocator *allocator) noexcept
                : m_allocator(allocator), m_connectionManager(nullptr), m_options(options), m_releaseInvoked(false)
            {
                const auto &connectionOptions = m_options.ConnectionOptions;
                AWS_FATAL_ASSERT(connectionOptions.HostName.size() > 0);
                AWS_FATAL_ASSERT(connectionOptions.Port > 0);

                aws_http_connection_manager_options managerOptions;
                AWS_ZERO_STRUCT(managerOptions);
                managerOptions.bootstrap = connectionOptions.Bootstrap->GetUnderlyingHandle();
                managerOptions.port = connectionOptions.Port;
                managerOptions.max_connections = m_options.MaxConnections;
                managerOptions.socket_options = &connectionOptions.SocketOptions.GetImpl();
                managerOptions.initial_window_size = connectionOptions.InitialWindowSize;

                // TODO needs to be generalized
                aws_http_connection_monitoring_options monitoringOptions;
                AWS_ZERO_STRUCT(monitoringOptions);
                monitoringOptions.allowable_throughput_failure_interval_seconds = 2;
                monitoringOptions.minimum_throughput_bytes_per_second = 1 * 1024;
                managerOptions.monitoring_options = &monitoringOptions;

                if (options.EnableBlockingShutdown)
                {
                    managerOptions.shutdown_complete_callback = s_shutdownCompleted;
                    managerOptions.shutdown_complete_user_data = this;
                }
                else
                {
                    m_shutdownPromise.set_value();
                }

                aws_http_proxy_options proxyOptions;
                AWS_ZERO_STRUCT(proxyOptions);
                if (connectionOptions.ProxyOptions)
                {
                    const auto &proxyOpts = connectionOptions.ProxyOptions;
                    proxyOptions.host = aws_byte_cursor_from_c_str(proxyOpts->HostName.c_str());
                    proxyOptions.port = proxyOpts->Port;
                    proxyOptions.auth_type = (enum aws_http_proxy_authentication_type)proxyOpts->AuthType;

                    if (proxyOpts->AuthType == AwsHttpProxyAuthenticationType::Basic)
                    {
                        proxyOptions.auth_username = aws_byte_cursor_from_c_str(proxyOpts->BasicAuthUsername.c_str());
                        proxyOptions.auth_password = aws_byte_cursor_from_c_str(proxyOpts->BasicAuthPassword.c_str());
                    }

                    if (proxyOpts->TlsOptions)
                    {
                        proxyOptions.tls_options =
                            const_cast<aws_tls_connection_options *>(proxyOpts->TlsOptions->GetUnderlyingHandle());
                    }

                    managerOptions.proxy_options = &proxyOptions;
                }

                if (connectionOptions.TlsOptions)
                {
                    managerOptions.tls_connection_options =
                        const_cast<aws_tls_connection_options *>(connectionOptions.TlsOptions->GetUnderlyingHandle());
                }
                managerOptions.host = aws_byte_cursor_from_c_str(connectionOptions.HostName.c_str());

                m_connectionManager = aws_http_connection_manager_new(allocator, &managerOptions);
            }

            HttpClientConnectionManager::~HttpClientConnectionManager()
            {
                if (!m_releaseInvoked)
                {
                    aws_http_connection_manager_release(m_connectionManager);
                    m_shutdownPromise.get_future().get();
                }
                m_connectionManager = nullptr;
            }

            size_t HttpClientConnectionManager::GetOpenConnectionCount()
            {
                return aws_http_connection_manager_get_open_connection_count(m_connectionManager);
            }

            bool HttpClientConnectionManager::AcquireConnection(
                const OnClientConnectionAvailable &onClientConnectionAvailable) noexcept
            {
                auto connectionManagerCallbackArgs = Aws::Crt::New<ConnectionManagerCallbackArgs>(m_allocator);
                if (!connectionManagerCallbackArgs)
                {
                    return false;
                }

                connectionManagerCallbackArgs->m_connectionManager = shared_from_this();
                connectionManagerCallbackArgs->m_onClientConnectionAvailable = onClientConnectionAvailable;

                aws_http_connection_manager_acquire_connection(
                    m_connectionManager, s_onConnectionSetup, connectionManagerCallbackArgs);
                return true;
            }

            std::future<void> HttpClientConnectionManager::InitiateShutdown() noexcept
            {
                m_releaseInvoked = true;
                aws_http_connection_manager_release(m_connectionManager);
                return m_shutdownPromise.get_future();
            }

            class ManagedConnection final : public HttpClientConnection
            {
              public:
                ManagedConnection(
                    aws_http_connection *connection,
                    std::shared_ptr<HttpClientConnectionManager> connectionManager)
                    : HttpClientConnection(connection, connectionManager->m_allocator),
                      m_connectionManager(std::move(connectionManager))
                {
                }

                ~ManagedConnection() override
                {
                    if (m_connection)
                    {
                        aws_http_connection_manager_release_connection(
                            m_connectionManager->m_connectionManager, m_connection);
                        m_connection = nullptr;
                    }
                }

              private:
                std::shared_ptr<HttpClientConnectionManager> m_connectionManager;
            };

            void HttpClientConnectionManager::s_onConnectionSetup(
                aws_http_connection *connection,
                int errorCode,
                void *userData) noexcept
            {
                auto callbackArgs = static_cast<ConnectionManagerCallbackArgs *>(userData);
                std::shared_ptr<HttpClientConnectionManager> manager = callbackArgs->m_connectionManager;
                auto callback = std::move(callbackArgs->m_onClientConnectionAvailable);

                Delete(callbackArgs, manager->m_allocator);

                if (errorCode)
                {
                    callback(nullptr, errorCode);
                    return;
                }

                auto allocator = manager->m_allocator;
                auto connectionRawObj = Aws::Crt::New<ManagedConnection>(manager->m_allocator, connection, manager);

                if (!connectionRawObj)
                {
                    aws_http_connection_manager_release_connection(manager->m_connectionManager, connection);
                    callback(nullptr, AWS_ERROR_OOM);
                    return;
                }
                auto connectionObj = std::shared_ptr<ManagedConnection>(
                    connectionRawObj,
                    [allocator](ManagedConnection *managedConnection) { Delete(managedConnection, allocator); });

                callback(connectionObj, AWS_OP_SUCCESS);
            }
        } // namespace Http
    }     // namespace Crt
} // namespace Aws
