/*
 *  Copyright (c) 2021, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.carbon.apimgt.thrift;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.wso2.carbon.apimgt.thrift.dto.DataBridgeRequestResponseStreamPublisherDTO;
import org.wso2.carbon.apimgt.thrift.dto.ExecutionTimeDTO;
import org.wso2.carbon.apimgt.thrift.dto.RequestResponseStreamDTO;
import org.wso2.carbon.databridge.agent.AgentHolder;
import org.wso2.carbon.databridge.agent.DataPublisher;
import org.wso2.carbon.databridge.agent.exception.DataEndpointAgentConfigurationException;
import org.wso2.carbon.databridge.agent.exception.DataEndpointAuthenticationException;
import org.wso2.carbon.databridge.agent.exception.DataEndpointConfigurationException;
import org.wso2.carbon.databridge.agent.exception.DataEndpointException;
import org.wso2.carbon.databridge.commons.exception.TransportException;
import org.wso2.carbon.databridge.commons.utils.DataBridgeCommonsUtils;

import java.io.FileReader;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * WSO2 API Manager Analytics - Event Client
 */
public class Client {

    private static final Log log = LogFactory.getLog(Client.class);

    private static final String REQUEST_RESPONSE_STREAM_NAME = "org.wso2.apimgt.statistics.request";
    private static final String VERSION = "3.0.0";
    private static final String eventDataFileName = "event-data.json";
    private static final String clientConfigFileName = "client-config.json";
    private static DataPublisher dataPublisher;
    private static int eventsPerSec = 0;
    private static int totalEventCount = 0;
    private static int eventPerSecond = 0;
    private static long firstTimestamp = 0L;
    private static ScheduledExecutorService scheduledExecutorService;
    private static int j = 0;
    private static int repeatCombination = 0;

    public static void main(String[] args) {

        DataPublisherUtil.setKeyStoreParams();
        DataPublisherUtil.setTrustStoreParams();

        try {
            log.info("Starting APIM Analytics Event Client");
            JSONObject configObject = readJsonFile(clientConfigFileName);
            createDataPublisher(configObject);

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                log.error(e);
            }
            JSONObject optionsObject = readJsonFile(eventDataFileName);
            JSONObject currentObject = new JSONObject();
            JSONArray resultsArray = new JSONArray();
            firstTimestamp = Instant.now().getEpochSecond();
            scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
            scheduledExecutorService.scheduleWithFixedDelay(new tpsLogger(), 1, 1, TimeUnit.MINUTES);
            publishCombinations(optionsObject, 0, resultsArray, currentObject);
            log.info("Events published successfully. Publisher will shutdown now");
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                log.error(e);
            }
            dataPublisher.shutdown();
            log.info("Analytics client ended");
        } catch (Throwable e) {
            log.error(e);
        }

    }

    private static void createDataPublisher(JSONObject configObject)
            throws DataEndpointAuthenticationException, DataEndpointAgentConfigurationException, TransportException,
            DataEndpointException, DataEndpointConfigurationException {

        String protocol = null;
        String receiverURL = (String) configObject.get("receiverURL");
        String authURL = (String) configObject.get("authURL");
        String username = (String) configObject.get("username");
        String password = (String) configObject.get("password");
        String configName = (String) configObject.get("agentConfigFileName");

        AgentHolder.setConfigPath(DataPublisherUtil.getConfFilePath(configName));
        dataPublisher = new DataPublisher(protocol, receiverURL, authURL, username, password);

        eventsPerSec = Integer.parseInt((String) configObject.get("eventsPerSec"));
        repeatCombination = Integer.parseInt((String) configObject.get("repeatCombination"));
    }

    private static void publishToServer(JSONObject dataObject) {
        j++;
        RequestResponseStreamDTO requestStream = new RequestResponseStreamDTO();
        requestStream.setUsername((String) dataObject.get("Username"));
        requestStream.setUserTenantDomain((String) dataObject.get("UserTenantDomain"));
        requestStream.setUserIp((String) dataObject.get("UserIp"));
        requestStream.setApiContext((String) dataObject.get("ApiContext"));
        requestStream.setApplicationConsumerKey((String) dataObject.get("ApplicationConsumerKey"));
        requestStream.setApplicationName((String) dataObject.get("ApplicationName"));
        requestStream.setApplicationId((String) dataObject.get("ApplicationId"));
        requestStream.setApplicationOwner((String) dataObject.get("ApplicationOwner"));
        requestStream.setApiContext((String) dataObject.get("ApiContext"));
        requestStream.setApiName((String) dataObject.get("ApiName"));
        requestStream.setApiVersion((String) dataObject.get("ApiVersion"));
        requestStream.setApiResourcePath((String) dataObject.get("ApiResourcePath"));
        requestStream.setApiResourceTemplate((String) dataObject.get("ApiResourceTemplate"));
        requestStream.setApiMethod((String) dataObject.get("ApiMethod"));
        requestStream.setApiCreator((String) dataObject.get("ApiCreator"));
        requestStream.setApiCreatorTenantDomain((String) dataObject.get("ApiCreatorTenantDomain"));
        requestStream.setApiTier((String) dataObject.get("ApiTier"));
        requestStream.setApiHostname((String) dataObject.get("ApiHostname"));
        requestStream.setUserAgent((String) dataObject.get("UserAgent"));
        requestStream.setServiceTime(Integer.parseInt((String) (dataObject.get("ServiceTime"))));
        requestStream.setRequestTimestamp(System.currentTimeMillis());
        requestStream.setThrottledOut(Boolean.parseBoolean((String) dataObject.get("ThrottledOut")));
        requestStream.setBackendTime(Integer.parseInt((String) dataObject.get("BackendTime")));
        requestStream.setResponseCacheHit(Boolean.parseBoolean((String) dataObject.get("ResponseCacheHit")));
        requestStream.setResponseSize(Integer.parseInt((String) dataObject.get("ResponseSize")));
        requestStream.setProtocol((String) dataObject.get("Protocol"));
        requestStream.setResponseCode(Integer.parseInt((String) dataObject.get("ResponseCode")));
        requestStream.setDestination((String) dataObject.get("Destination"));
        requestStream.setMetaClientType((String) dataObject.get("MetaClientType"));
        requestStream.setResponseTime(Integer.parseInt((String) dataObject.get("ResponseTime")));
        requestStream.setGatewayType((String) dataObject.get("GatewayType"));
        requestStream.setCorrelationID((String) dataObject.get("CorrelationID"));
        requestStream.setLabel((String) dataObject.get("Label"));
        ExecutionTimeDTO executionTimeDTO = new ExecutionTimeDTO();
        executionTimeDTO.setSecurityLatency(Integer.parseInt((String) dataObject.get("SecurityLatency")));
        executionTimeDTO.setThrottlingLatency(Integer.parseInt((String) dataObject.get("ThrottlingLatency")));
        executionTimeDTO
                .setRequestMediationLatency(Integer.parseInt((String) dataObject.get("RequestMediationLatency")));
        executionTimeDTO
                .setResponseMediationLatency(Integer.parseInt((String) dataObject.get("ResponseMediationLatency")));
        executionTimeDTO.setBackEndLatency(Integer.parseInt((String) dataObject.get("BackEndLatency")));
        executionTimeDTO.setOtherLatency(Integer.parseInt((String) dataObject.get("OtherLatency")));
        requestStream.setExecutionTime(executionTimeDTO);

        DataBridgeRequestResponseStreamPublisherDTO dataBridgeRequestPublisherDTO =
                new DataBridgeRequestResponseStreamPublisherDTO(requestStream);
        String streamId = DataBridgeCommonsUtils.generateStreamId(REQUEST_RESPONSE_STREAM_NAME, VERSION);
        dataPublisher.tryPublish(streamId, System.currentTimeMillis(),
                (Object[]) dataBridgeRequestPublisherDTO.createMetaData(), null,
                (Object[]) dataBridgeRequestPublisherDTO.createPayload());
        totalEventCount = totalEventCount + 1;
        eventPerSecond = eventPerSecond + 1;
        if (j % eventsPerSec == 0) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                log.error(e);
            }
        }
    }

    private static JSONArray publishCombinations(JSONObject options, int optionIndex, JSONArray results,
                                                 JSONObject current) {

        String[] allKeys = getKeysArray(options);
        String optionKey = allKeys[optionIndex];

        JSONArray vals = (JSONArray) options.get(optionKey);
        for (int i = 0; i < vals.size(); i++) {
            current.put(optionKey, vals.get(i));
            if (optionIndex + 1 < allKeys.length) {
                publishCombinations(options, optionIndex + 1, results, current);
            } else {
                JSONObject res = current;
                for (int j = 0; j < repeatCombination; j++) {
                    publishToServer(res);
                }
            }
        }
        return results;

    }

    private static String[] getKeysArray(JSONObject jsonObject) {

        List<String> keysList = new ArrayList<String>();
        for (Object keyStr : jsonObject.keySet()) {
            keysList.add((String) keyStr);
        }
        return keysList.toArray(new String[keysList.size()]);
    }

    private static JSONObject readJsonFile(String jsonFileName) {

        JSONParser jsonParser = new JSONParser();
        JSONObject eventData = null;
        try (FileReader reader = new FileReader(DataPublisherUtil.getConfFilePath(jsonFileName))) {
            Object obj = jsonParser.parse(reader);
            eventData = (JSONObject) obj;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return eventData;
    }

    static class tpsLogger implements Runnable {
        long secondTimestamp = 0L;

        public void run() {

            secondTimestamp = Instant.now().getEpochSecond();
            log.info("TPS = \"" + eventPerSecond / (secondTimestamp - firstTimestamp) + "\" ," +
                    " Total Event Count = " + totalEventCount);
            firstTimestamp = secondTimestamp;
            eventPerSecond = 0;

        }
    }
}
