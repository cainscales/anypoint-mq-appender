package com.github.cainscales;

import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import okhttp3.*;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.appender.AppenderLoggingException;

import java.io.IOException;
import java.util.*;

public class AnypointMQSender extends TimerTask {

    private static final String ANYPOINT_MQ_AUTH_URL = "https://mq-%s.anypoint.mulesoft.com/api/v1/authorize";
    private static final String ANYPOINT_MQ_PUBLISH_URL =
            "https://mq-%s.anypoint.mulesoft.com/api/v1/organizations/%s/environments/%s/destinations/%s/messages";
    private String token;

    private final String name;
    private final String clientId;
    private final String clientSecret;
    private final String region;
    private final String destination;
    // To handle the lifecycle LOGGER
    private final Logger LOGGER;

    private static final OkHttpClient sharedOkHttpClient = new OkHttpClient();


    private String publishUrl;
    private String environmentId;
    private String organizationId;
    private Timer batchSendTimer;
    private final int batchCount;
    private final int batchInterval;
    private LinkedList<AnypointMQMessage> batchMessages = new LinkedList<>();

    private static final MediaType MEDIA_TYPE_APPLICATION_JSON = MediaType.get("application/json");
    private final Gson gson;


    public AnypointMQSender(
            String clientId, String clientSecret, String region, String destination,
            int batchCount, int batchInterval, Logger LOGGER, String name
    ) {
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.region = region;
        this.destination = destination;
        this.batchCount = batchCount;
        this.batchInterval = batchInterval;
        this.name = name;
        this.gson = new Gson();
        this.LOGGER = LOGGER;
        this.acquireAuthorizationToken();
        if (batchInterval > 0 && this.batchCount > 1) {
            this.batchSendTimer = new Timer();
            this.batchSendTimer.schedule(this, this.batchInterval, this.batchInterval);
        }
    }

    private boolean acquireAuthorizationToken() {
        String _auth_url = String.format(ANYPOINT_MQ_AUTH_URL, this.region);

        RequestBody _request_body = new FormBody.Builder()
                .addEncoded("client_id", this.clientId)
                .addEncoded("client_secret", this.clientSecret)
                .addEncoded("grant_type", "client_credentials")
                .build();

        Request _auth_request = new Request.Builder()
                .url(_auth_url)
                .post(_request_body)
                .build();

        try (Response _auth_token_response = sharedOkHttpClient.newCall(_auth_request).execute()) {
            if (_auth_token_response.code() != 200) {
                LOGGER.error("Error on acquiring Access Token: " + _auth_token_response.code() + " " + _auth_token_response.body().string());
                return false;
            }
            String _response_body = _auth_token_response.body().string();

            LinkedTreeMap _result = gson.fromJson(_response_body, LinkedTreeMap.class);
            LinkedTreeMap _simple_client = (LinkedTreeMap)_result.get("simple_client");

            this.environmentId = _simple_client.get("envId").toString();
            this.organizationId = _simple_client.get("orgId").toString();
            this.token = _result.get("access_token").toString();
            this.publishUrl = String.format(ANYPOINT_MQ_PUBLISH_URL, this.region, this.organizationId, this.environmentId, this.destination);

            return true;
        }
        catch (Exception e) {
            LOGGER.error(e);
            return false;
        }
    }

    public void send(AnypointMQMessage message) {
        if (this.batchCount > 1) {
            this.batchMessages.add(message);
            if (this.batchMessages.size() >= this.batchCount) {
                // Do send
                flushBatchMessages();
            }
            return;
        }
        this.sendSingleMessage(message);
    }

    private void sendSingleMessage(AnypointMQMessage message) {
        LinkedList<AnypointMQMessage> _to_send = new LinkedList<>();
        _to_send.add(message);
        flushBatchMessages(_to_send);
    }

    private void flushBatchMessages() {
        flushBatchMessages(this.batchMessages);
        this.batchMessages = new LinkedList<>();
    }

    private void flushBatchMessages(LinkedList<AnypointMQMessage> messages) {
        String _json_anypoint_mq_body = this.gson.toJson(messages);

        RequestBody _send_messages_body = RequestBody.create(_json_anypoint_mq_body, MEDIA_TYPE_APPLICATION_JSON);

        Request _send_messages_request = new Request.Builder()
                .url(this.publishUrl)
                .addHeader("Authorization", "Bearer " + this.token)
                .put(_send_messages_body)
                .build();


        try (Response _send_messages_response = sharedOkHttpClient.newCall(_send_messages_request).execute()) {
            if (_send_messages_response.isSuccessful()) {
                return;
            }
            if (_send_messages_response.code() != 401) {
                // We didn't get an authorization error but something went wrong.
                throw new AppenderLoggingException(_send_messages_response.body().string());
            }

            if (!this.acquireAuthorizationToken()) {
                LOGGER.error("AnypointMQAppender unable to authorize!");
                throw new AppenderLoggingException("AnypointMQAppender unable to authorize.");
            }

            _send_messages_request = new Request.Builder()
                    .url(this.publishUrl)
                    .addHeader("Authorization", "Bearer " + this.token)
                    .put(_send_messages_body)
                    .build();

            try (Response _send_messages_response_reauthed = sharedOkHttpClient.newCall(_send_messages_request).execute()) {
                if (!_send_messages_response_reauthed.isSuccessful()) {
                    throw new AppenderLoggingException(_send_messages_response_reauthed.body().string());
                }
                return;
            }
        }
        catch (IOException e) {
            throw new AppenderLoggingException(e.toString());
        }
    }

    @Override
    public void run() {
        if (this.batchMessages.size() > 0) {
            flushBatchMessages();
        }
    }

    public void stop() {
        if (this.batchSendTimer != null) {
            this.batchSendTimer.cancel();
        }
        if (this.batchMessages.size() > 0) {
            flushBatchMessages();
        }
    }
}
