/*
 * Copyright 2010-2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
#pragma once

#include "MetricsPublisher.h"
#include <aws/crt/Types.h>

/*
 * TransferState represents an individual transfer, which can be an entire object, or an individual part of a multipart
 * object. It holds onto arrays that track upload/download metrics, which can be sent to the metrics publisher using the
 * flush methods. Metric values that are passed to the TransferState are distributed over time, and are combined at a
 * one second granularity. Functionality currently assumes that the transfer operation is happening linearly, and that
 * while many other transfers may be happening in parallel, this particular transfer is not being broken up into
 * parallel operations.
 */
class TransferState : public std::enable_shared_from_this<TransferState>
{
  public:
    TransferState();
    TransferState(int32_t partIndex);

    int32_t GetPartIndex() const { return m_partIndex; }
    int32_t GetPartNumber() const { return m_partIndex + 1; }

    void SetConnection(const std::shared_ptr<Aws::Crt::Http::HttpClientConnection> &connection);

    std::shared_ptr<Aws::Crt::Http::HttpClientConnection> GetConnection() const { return m_connection.lock(); }

    const Aws::Crt::String &GetHostAddress() { return m_hostAddress; }

    bool HasDataUpMetrics() { return !m_uploadMetrics.empty(); }

    void QueueDataUpMetric(uint64_t dataUsed) { m_queuedDataUp = dataUsed; }

    void ConsumeQueuedDataUpMetric()
    {
        if (m_queuedDataUp == 0)
        {
            return;
        }

        AddDataUpMetric(m_queuedDataUp);
        m_queuedDataUp = 0ULL;
    }

    /*
     * Flags this is a success or failure, which will be reported as a metric on a flush
     * of one of up or down metrics.
     */
    void SetTransferSuccess(bool success);

    /*
     * Initializes data up metric, setting a start time for when the upload started.
     */
    void InitDataUpMetric();

    /*
     * Initializes data down metric, setting a start time for when the download started.
     */
    void InitDataDownMetric();

    /*
     * Notify the transfer state that a chunk of data finished uploading.
     */
    void AddDataUpMetric(uint64_t dataUp);

    /*
     * Notify the transfer rate that a chunk of data finished downloading.
     */
    void AddDataDownMetric(uint64_t dataDown);

    /*
     * Send upload metrics to the metrics publisher and clear our local copy.
     * Also reports success or failure metric during this time.
     */
    void FlushDataUpMetrics(const std::shared_ptr<MetricsPublisher> &publisher);

    /*
     * Send download metrics to the metrics publisher and clear the local copy.
     * Also reports success or failure metric during this time.
     */
    void FlushDataDownMetrics(const std::shared_ptr<MetricsPublisher> &publisher);

    const Aws::Crt::String &GetAmzRequestId() const { return m_amzRequestId; }
    const Aws::Crt::String &GetAmzId2() const { return m_amzId2; }

    void ProcessHeaders(const Aws::Crt::Http::HttpHeader *headersArray, size_t headersCount);

  private:
    static std::atomic<uint64_t> s_nextTransferId;

    uint64_t m_dataUsedRateSum;
    uint64_t m_dataUsedRateTimestamp;

    int32_t m_partIndex;
    uint64_t m_transferId;
    uint64_t m_queuedDataUp;
    uint32_t m_transferSuccess : 1;

    Aws::Crt::String m_amzRequestId;
    Aws::Crt::String m_amzId2;
    Aws::Crt::String m_hostAddress;
    Aws::Crt::Vector<Metric> m_uploadMetrics;
    Aws::Crt::Vector<Metric> m_downloadMetrics;

    std::weak_ptr<Aws::Crt::Http::HttpClientConnection> m_connection;
    std::weak_ptr<MetricsPublisher> m_publisher;

    static uint64_t GetNextTransferId();

    void DistributeDataUsedOverSeconds(
        Aws::Crt::Vector<Metric> &metrics,
        MetricName metricName,
        uint64_t beginTime,
        double dataUsed);

    void PushDataUsedForSecondAndAggregate(
        Aws::Crt::Vector<Metric> &metrics,
        MetricName metricName,
        uint64_t timestamp,
        double dataUsed);

    void PushDataMetric(Aws::Crt::Vector<Metric> &metrics, MetricName metricName, double dataUsed);

    void FlushMetricsVector(const std::shared_ptr<MetricsPublisher> &publisher, Aws::Crt::Vector<Metric> &metrics);

    void ResetRateTracking();

    void UpdateRateTracking(uint64_t dataUsed, bool forceFlush);
};