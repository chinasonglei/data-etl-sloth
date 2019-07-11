package com.beadwallet.utils.azkaban;

import com.beadwallet.utils.config.ConfigUtil;
import com.beadwallet.utils.ssl.SSLUtil;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * Azkaban接口Util
 *
 * @author QuChunhui 2019/01/28
 */
public class AzkabanApiUtil {
    private static Logger logger = LoggerFactory.getLogger(AzkabanApiUtil.class);
    private static AzkabanApiUtil azkabanApiUtil = null;
    private static RestTemplate restTemplate = null;
    private static String sessionId = null;
    private static String url = null;

    private AzkabanApiUtil() {
        //Nothing
    }

    /**
     * 获得AzkabanApiUtil实例
     */
    public static AzkabanApiUtil getInstance() {
        //对象初始化
        if (azkabanApiUtil != null) {
            return azkabanApiUtil;
        }
        azkabanApiUtil = new AzkabanApiUtil();

        //Azkaban连接地址
        url = ConfigUtil.getProperties(ConfigUtil.AZKABAN_URL);

        //绕过SSL验证
        SSLUtil.disableChecks();

        //初始化RestTemplate
        restTemplate = getRestTemplate();

        //获取Session
        sessionId = login();

        return azkabanApiUtil;
    }

    /**
     * 初始化RestTemplate对象
     *
     * @return RestTemplate
     */
    private static RestTemplate getRestTemplate() {
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(600000);
        requestFactory.setReadTimeout(600000);
        return new RestTemplate(requestFactory);
    }

    /**
     * 连接Azkaban并获取Session
     *
     * @return sessionId
     */
    private static String login() {
        //Azkaban用户名
        String username = ConfigUtil.getProperties(ConfigUtil.AZKABAN_USERNAME);
        if (username == null) {
            logger.error(String.format(
                "property %s is not exist.", ConfigUtil.AZKABAN_USERNAME));
            return null;
        }
        //Azkaban密码
        String password = ConfigUtil.getProperties(ConfigUtil.AZKABAN_PASSWORD);
        if (password == null) {
            logger.error(String.format(
                "property %s is not exist.", ConfigUtil.AZKABAN_PASSWORD));
            return null;
        }

        //连接Azkaban，获取session
        HttpHeaders hs = new HttpHeaders();
        hs.add("Content-Type",
            "application/x-www-form-urlencoded; charset=utf-8");
        hs.add("X-Requested-With", "XMLHttpRequest");
        LinkedMultiValueMap<String, String> linkedMultiValueMap = new LinkedMultiValueMap<>();
        linkedMultiValueMap.add("action", "login");
        linkedMultiValueMap.add("username", username);
        linkedMultiValueMap.add("password", password);
        HttpEntity<MultiValueMap<String, String>>
            httpEntity = new HttpEntity<>(linkedMultiValueMap, hs);
        logger.info("make a azkaban request. HttpEntity=" + httpEntity.toString());

        //A sample response:
        //{
        //    "status" : "success",
        //    "session.id" : "c001aba5-a90f-4daf-8f11-62330d034c0a"
        //}
        String result = restTemplate.postForObject(url, httpEntity, String.class);

        //返回Session ID
        JsonObject jsonObject = new Gson().fromJson(result, JsonObject.class);
        if (!"success".equals(jsonObject.get("status").getAsString())) {
            logger.error(String.format(
                "request=%s, response=%s", httpEntity.toString(), jsonObject.toString()));
            return null;
        }

        String sessionId = new Gson().fromJson(
            result, JsonObject.class).get("session.id").getAsString();
        logger.info("login success. session.id=" + sessionId);
        return sessionId;
    }

    /**
     * 创建一个project
     *
     * @param project Project名称
     */
    public boolean createProject(String project) {
        String api = "Create a Project";
        HttpHeaders hs = getHttpHeadersPost();

        LinkedMultiValueMap<String, String> linkedMultiValueMap = new LinkedMultiValueMap<>();
        linkedMultiValueMap.add("session.id", sessionId);
        linkedMultiValueMap.add("action", "create");
        linkedMultiValueMap.add("name", project);
        linkedMultiValueMap.add("description", project);

        HttpEntity<MultiValueMap<String, String>> httpEntity = new HttpEntity<>(linkedMultiValueMap, hs);
        //A sample response:
        //{
        //    "status":"success",
        //    "path":"manager?project=aaaa",
        //    "action":"redirect"
        //}
        String result = restTemplate.postForObject(url + "/manager", httpEntity, String.class);

        JsonObject jsonObject = toJson(result);
        if ("success".equals(jsonObject.get("status").getAsString())) {
            logger.info(formatPostLog(api, httpEntity, jsonObject));
            return true;
        } else {
            logger.error(formatPostLog(api, httpEntity, jsonObject));
            return false;
        }
    }

    /**
     * 删除一个project
     *
     * @param project Project名称
     */
    public boolean deleteProject(String project) {
        String api = "Delete a Project";
        HttpHeaders hs = getHttpHeadersGet();

        Map<String, String> map = new HashMap<>();
        map.put("id", sessionId);
        map.put("project", project);

        ResponseEntity<String> exchange = restTemplate.exchange(
                url + "/manager?session.id={id}&delete=true&project={project}",
                HttpMethod.GET, new HttpEntity<String>(hs), String.class, map);

        if (HttpStatus.OK.equals(exchange.getStatusCode())) {
            logger.info(formatGetLog(api, map));
            return true;
        } else {
            logger.error(formatGetLog(api, map));
            return false;
        }
    }

    /**
     * 上传依赖文件zip包
     *
     * @param project Project名称
     * @param filePath Zip文件路径
     */
    public boolean uploadZip(String project, String filePath) {
        String api = "Upload a Project Zip";
        HttpHeaders hs = getHttpHeadersPost();

        FileSystemResource resource = new FileSystemResource(new File(filePath));
        LinkedMultiValueMap<String, Object> linkedMultiValueMap = new LinkedMultiValueMap<>();
        linkedMultiValueMap.add("session.id", sessionId);
        linkedMultiValueMap.add("ajax", "upload");
        linkedMultiValueMap.add("project", project);
        linkedMultiValueMap.add("file", resource);

        HttpEntity<MultiValueMap<String, Object>> httpEntity = new HttpEntity<>(linkedMultiValueMap, hs);
        //A response sample:
        //{
        //    "error" : "Installation Failed.\nError unzipping file.",
        //    "projectId" : "192",
        //    "version" : "1"
        //}
        String result = restTemplate.postForObject(url + "/manager", linkedMultiValueMap, String.class);

        JsonObject jsonObject = toJson(result);
        if (!jsonObject.has("error")) {
            logger.info(formatPostLog(api, httpEntity, jsonObject));
            return true;
        } else {
            logger.error(formatPostLog(api, httpEntity, jsonObject));
            return false;
        }
    }

    /**
     * 获取一个project的流ID
     *
     * @param project Project名称
     */
    public boolean fetchFlowAProject(String project) {
        String api = "Fetch Flows of a Project";
        HttpHeaders hs = getHttpHeadersGet();

        Map<String, String> map = new HashMap<>();
        map.put("id", sessionId);
        map.put("project", project);

        //A response sample:
        //{
        //    "project" : "test-azkaban",
        //    "projectId" : 192,
        //    "flows" : [ {
        //    "flowId" : "test"
        //}, {
        //    "flowId" : "test2"
        //} ]
        //}
        ResponseEntity<String> exchange = restTemplate.exchange(
                url + "/manager?session.id={id}&ajax=fetchprojectflows&project={project}",
                HttpMethod.GET, new HttpEntity<String>(hs), String.class, map);

        if (HttpStatus.OK.equals(exchange.getStatusCode())) {
            logger.info(formatGetLog(api, map, exchange));
            return true;
        } else {
            logger.error(formatGetLog(api, map, exchange));
            return false;
        }
    }

    /**
     * 获取一个job的流结构依赖关系
     *
     * @param project Project名称
     * @param flow Flow名称
     */
    public boolean fetchFlow(String project, String flow){
        String api = "Fetch Jobs of a Flow";
        HttpHeaders hs = getHttpHeadersGet();

        Map<String, String> map = new HashMap<>();
        map.put("id", sessionId);
        map.put("project", project);
        map.put("flow", flow);

        //A response sample:
        //{
        //    "project" : "azkaban-test-project",
        //    "nodes" : [ {
        //    "id" : "test-final",
        //        "type" : "command",
        //        "in" : [ "test-job-3" ]
        //}, {
        //    "id" : "test-job-start",
        //        "type" : "java"
        //}, {
        //    "id" : "test-job-3",
        //        "type" : "java",
        //        "in" : [ "test-job-2" ]
        //}, {
        //    "id" : "test-job-2",
        //        "type" : "java",
        //        "in" : [ "test-job-start" ]
        //} ],
        //    "flow" : "test",
        //    "projectId" : 192
        //}
        ResponseEntity<String> exchange = restTemplate.exchange(
                url + "/manager?session.id={id}&ajax=fetchflowgraph&project={project}&flow={flow}",
                HttpMethod.GET, new HttpEntity<String>(hs), String.class, map);

        if (HttpStatus.OK.equals(exchange.getStatusCode())) {
            logger.info(formatGetLog(api, map, exchange));
            return true;
        } else {
            logger.error(formatGetLog(api, map, exchange));
            return false;
        }
    }

    /**
     * Fetch Executions of a Flow 获取执行的project 列表
     */
    public boolean fetchEXEFlow(String project, String flow) {
        String api = "Fetch Executions of a Flow";
        HttpHeaders hs = getHttpHeadersGet();

        Map<String, String> map = new HashMap<>();
        map.put("id", sessionId);
        map.put("project", project);
        map.put("flow", flow);
        map.put("start", "1");
        map.put("length", "30");

        //A response sample:
        //{
        //    "executions" : [ {
        //    "startTime" : 1407779928865,
        //        "submitUser" : "1",
        //        "status" : "FAILED",
        //        "submitTime" : 1407779928829,
        //        "execId" : 306,
        //        "projectId" : 192,
        //        "endTime" : 1407779950602,
        //        "flowId" : "test"
        //}, {
        //    "startTime" : 1407779877807,
        //        "submitUser" : "1",
        //        "status" : "FAILED",
        //        "submitTime" : 1407779877779,
        //        "execId" : 305,
        //        "projectId" : 192,
        //        "endTime" : 1407779899599,
        //        "flowId" : "test"
        //}, {
        //    "startTime" : 1407779473354,
        //        "submitUser" : "1",
        //        "status" : "FAILED",
        //        "submitTime" : 1407779473318,
        //        "execId" : 304,
        //        "projectId" : 192,
        //        "endTime" : 1407779495093,
        //        "flowId" : "test"
        //} ],
        //    "total" : 16,
        //    "project" : "azkaban-test-project",
        //    "length" : 3,
        //    "from" : 0,
        //    "flow" : "test",
        //    "projectId" : 192
        //}
        ResponseEntity<String> exchange = restTemplate.exchange(
                url + "/manager?session.id={id}&ajax=fetchFlowExecutions&project={project}&flow={flow}&start={start}&length={length}",
                HttpMethod.GET, new HttpEntity<String>(hs), String.class, map);

        if (HttpStatus.OK.equals(exchange.getStatusCode())) {
            logger.info(formatGetLog(api, map, exchange));
            return true;
        } else {
            logger.error(formatGetLog(api, map, exchange));
            return false;
        }
    }

    /**
     * Fetch Running Executions of a Flow 获取正在执行的流id
     */
    public boolean fetchRunningEXEFlow(String project, String flow) {
        String api = "Fetch Running Executions of a Flow";
        HttpHeaders hs = getHttpHeadersGet();

        Map<String, String> map = new HashMap<>();
        map.put("id", sessionId);
        map.put("project", project);
        map.put("flow", flow);

        //A response sample:
        //{
        //    "execIds": [301, 302]
        //}
        ResponseEntity<String> exchange = restTemplate.exchange(
                url + "/executor?session.id={id}&ajax=getRunning&project={project}&flow={flow}",
                HttpMethod.GET, new HttpEntity<String>(hs), String.class, map);

        if (HttpStatus.OK.equals(exchange.getStatusCode())) {
            logger.info(formatGetLog(api, map, exchange));
            return true;
        } else {
            logger.error(formatGetLog(api, map, exchange));
            return false;
        }
    }

    /**
     * Execute a Flow 执行一个流 还有很多其他参数 具体参考api
     */
    public boolean executeFlow(String project, String flow) {
        String api = "Execute a Flow";
        HttpHeaders hs = getHttpHeadersGet();

        Map<String, String> map = new HashMap<>();
        map.put("id", sessionId);
        map.put("project", project);
        map.put("flow", flow);

        //A response sample:
        //{
        //    message: "Execution submitted successfully with exec id 295",
        //    project: "foo-demo",
        //    flow: "test",
        //    execid: 295
        //}
        ResponseEntity<String> exchange = restTemplate.exchange(
                url + "/executor?session.id={id}&ajax=executeFlow&project={project}&flow={flow}",
                HttpMethod.GET, new HttpEntity<String>(hs), String.class, map);

        if (HttpStatus.OK.equals(exchange.getStatusCode())) {
            logger.debug(formatGetLog(api, map, exchange));
            return true;
        } else {
            logger.error(formatGetLog(api, map, exchange));
            return false;
        }
    }

    /**
     * Cancel a Flow Execution 中断一个执行流
     */
    public boolean cancelEXEaFlow(String execId) {
        String api = "Cancel a Flow Execution";
        HttpHeaders hs = getHttpHeadersGet();

        Map<String, String> map = new HashMap<>();
        map.put("id", sessionId);
        map.put("execid", execId);

        //A response sample if succeeds:
        //{ }
        //A response sample if fails:
        //{
        //    "error" : "Execution 302 of flow test isn't running."
        //}
        ResponseEntity<String> exchange = restTemplate.exchange(
                url + "/executor?session.id={id}&ajax=cancelFlow&execid={execid}",
                HttpMethod.GET, new HttpEntity<String>(hs), String.class, map);

        if (HttpStatus.OK.equals(exchange.getStatusCode())) {
            logger.debug(formatGetLog(api, map, exchange));
            return true;
        } else {
            logger.error(formatGetLog(api, map, exchange));
            return false;
        }
    }

    /**
     * Schedule a period-based Flow 创建调度任务
     */
    public boolean scheduleEXEaFlow(
            String project, String projectId, String flow, String scheduleTime,
            String scheduleDate, String flowName, String is_recurring) {
        String api = "Schedule a period-based Flow";
        HttpHeaders hs = getHttpHeadersPost();

        LinkedMultiValueMap<String, String> linkedMultiValueMap = new LinkedMultiValueMap<>();
        linkedMultiValueMap.add("session.id", sessionId);
        linkedMultiValueMap.add("ajax", "scheduleFlow");
        linkedMultiValueMap.add("projectName", project);
        linkedMultiValueMap.add("projectId", projectId);

        linkedMultiValueMap.add("flow", flow);
        linkedMultiValueMap.add("scheduleTime", scheduleTime);
        linkedMultiValueMap.add("scheduleDate", scheduleDate);
        linkedMultiValueMap.add("flowName", flowName);

        // 是否循环
        linkedMultiValueMap.add("is_recurring", is_recurring);

        // 循环周期 天 年 月等
        // M Months
        // w Weeks
        // d Days
        // h Hours
        // m Minutes
        // s Seconds
        linkedMultiValueMap.add("period", "d");

        HttpEntity<MultiValueMap<String, String>> httpEntity = new HttpEntity<>(linkedMultiValueMap, hs);
        //An example success response:
        //{
        //    "message" : "PROJECT_NAME.FLOW_NAME scheduled.",
        //    "status" : "success"
        //}
        //An example failure response:
        //{
        //    "message" : "Permission denied. Cannot execute FLOW_NAME",
        //    "status" : "error"
        //}
        //An example failure response for invalid schedule period:
        //{
        //    "message" : "PROJECT_NAME.FLOW_NAME scheduled.",
        //    "error" : "Invalid schedule period unit 'A",
        //    "status" : "success"
        //}
        String result = restTemplate.postForObject(url + "/schedule", httpEntity, String.class);

        JsonObject jsonObject = toJson(result);
        if ("success".equals(jsonObject.get("status").getAsString())) {
            logger.info(formatPostLog(api, httpEntity, jsonObject));
            return true;
        } else {
            logger.error(formatPostLog(api, httpEntity, jsonObject));
            return false;
        }
    }

    /**
     * Flexible scheduling using Cron 通过cron表达式调度执行 创建调度任务
     */
    public boolean scheduleByCronEXEaFlow(String project, String flow, String cron) {
        String api = "Flexible scheduling using Cron";
        HttpHeaders hs = getHttpHeadersPost();

        LinkedMultiValueMap<String, String> linkedMultiValueMap = new LinkedMultiValueMap<>();
        linkedMultiValueMap.add("session.id", sessionId);
        linkedMultiValueMap.add("ajax", "scheduleCronFlow");
        linkedMultiValueMap.add("projectName", project);
        linkedMultiValueMap.add("flow", flow);
        linkedMultiValueMap.add("cronExpression", cron);

        HttpEntity<MultiValueMap<String, String>> httpEntity = new HttpEntity<>(linkedMultiValueMap, hs);
        //An example success response:
        //{
        //    "message" : "PROJECT_NAME.FLOW_NAME scheduled.",
        //    "scheduleId" : SCHEDULE_ID,
        //    "status" : "success"
        //}
        //An example failure response:
        //{
        //    "message" : "Cron expression must exist.",
        //    "status" : "error"
        //}
        //An example failure response for invalid cron expression:
        //{
        //    "message" : "This expression <*****> can not be parsed to quartz cron.",
        //    "status" : "error"
        //}
        String result = restTemplate.postForObject(url + "/schedule", httpEntity, String.class);

        JsonObject jsonObject = toJson(result);
        if ("success".equals(jsonObject.get("status").getAsString())) {
            logger.debug(formatPostLog(api, httpEntity, jsonObject));
            return true;
        } else {
            logger.error(formatPostLog(api, httpEntity, jsonObject));
            return false;
        }
    }

    /**
     * Fetch a Schedule 获取一个调度器job的信息 根据project的id 和 flowId
     */
    public boolean fetchScheduleInfo(String projectId, String flowId) {
        String api = "Fetch a Schedule";
        HttpHeaders hs = getHttpHeadersGet();

        Map<String, String> map = new HashMap<>();
        map.put("id", sessionId);
        map.put("projectId", projectId);
        map.put("flowId", flowId);

        //An example success response:
        //{
        //    "schedule" : {
        //    "cronExpression" : "0 * 9 ? * *",
        //        "nextExecTime" : "2017-04-01 09:00:00",
        //        "period" : "null",
        //        "submitUser" : "azkaban",
        //        "executionOptions" : {
        //        "notifyOnFirstFailure" : false,
        //            "notifyOnLastFailure" : false,
        //            "failureEmails" : [ ],
        //        "successEmails" : [ ],
        //        "pipelineLevel" : null,
        //            "queueLevel" : 0,
        //            "concurrentOption" : "skip",
        //            "mailCreator" : "default",
        //            "memoryCheck" : true,
        //            "flowParameters" : {
        //            },
        //            "failureAction" : "FINISH_CURRENTLY_RUNNING",
        //                "failureEmailsOverridden" : false,
        //                "successEmailsOverridden" : false,
        //                "pipelineExecutionId" : null,
        //                "disabledJobs" : [ ]
        //        },
        //        "scheduleId" : "3",
        //            "firstSchedTime" : "2017-03-31 11:45:21"
        //    }
        //}
        //If there is no schedule, empty response returns.
        //{ }
        ResponseEntity<String> exchange = restTemplate.exchange(
                url + "/schedule?session.id={id}&ajax=fetchSchedule&projectId={projectId}&flowId={flowId}",
                HttpMethod.GET, new HttpEntity<String>(hs), String.class, map);

        if (HttpStatus.OK.equals(exchange.getStatusCode())) {
            logger.info(formatGetLog(api, map, exchange));
            return true;
        } else {
            logger.error(formatGetLog(api, map, exchange));
            return false;
        }
    }

    /**
     * Unschedule a Flow 取消一个流的调度
     */
    public boolean unScheduleFlow(String scheduleId) {
        String api = "Unschedule a Flow";
        HttpHeaders hs = getHttpHeadersPost();

        LinkedMultiValueMap<String, String> linkedMultiValueMap = new LinkedMultiValueMap<>();
        linkedMultiValueMap.add("session.id", sessionId);
        linkedMultiValueMap.add("action", "removeSched");
        linkedMultiValueMap.add("scheduleId", scheduleId);

        HttpEntity<MultiValueMap<String, String>> httpEntity = new HttpEntity<>(linkedMultiValueMap, hs);
        //An example success response:
        //{
        //    "message" : "flow FLOW_NAME removed from Schedules.",
        //    "status" : "success"
        //}
        //An example failure response:
        //{
        //    "message" : "Schedule with ID SCHEDULE_ID does not exist",
        //    "status" : "error"
        //}
        String result = restTemplate.postForObject(url + "/schedule", httpEntity, String.class);

        JsonObject jsonObject = toJson(result);
        if ("success".equals(jsonObject.get("status").getAsString())) {
            logger.info(formatPostLog(api, httpEntity, jsonObject));
            return true;
        } else {
            logger.error(formatPostLog(api, httpEntity, jsonObject));
            return false;
        }
    }

    /**
     * Pause a Flow Execution 暂停一个执行流
     */
    public boolean pauseSchedule(String execId) {
        String api = "Pause a Flow Execution";
        HttpHeaders hs = getHttpHeadersGet();

        Map<String, String> map = new HashMap<>();
        map.put("id", sessionId);
        map.put("execid", execId);

        //An example success response:
        //{ }
        //An example failure response:
        //{
        //    "error" : "azkaban.scheduler.ScheduleManagerException: Unable to parse duration for a SLA that needs to take actions!"
        //}
        ResponseEntity<String> exchange = restTemplate.exchange(
                url + "/executor?session.id={id}&ajax=pauseFlow&execid={execid}",
                HttpMethod.GET, new HttpEntity<String>(hs), String.class, map);

        if (HttpStatus.OK.equals(exchange.getStatusCode())) {
            logger.info(formatGetLog(api, map, exchange));
            return true;
        } else {
            logger.error(formatGetLog(api, map, exchange));
            return false;
        }
    }

    /**
     * Resume a Flow Execution 重新执行一个执行流
     */
    public boolean resumeSchedule(String execId) {
        String api = "Resume a Flow Execution";
        HttpHeaders hs = getHttpHeadersGet();

        Map<String, String> map = new HashMap<>();
        map.put("id", sessionId);
        map.put("execid", execId);

        //A response sample (if succeeds, or resumeFlow is called multiple times):
        //{ }
        //A response sample (if fails, only when the flow is not actually running):
        //{
        //    "error" : "Execution 303 of flow test isn't running."
        //}
        ResponseEntity<String> exchange = restTemplate.exchange(
                url + "/executor?session.id={id}&ajax=resumeFlow&execid={execid}",
                HttpMethod.GET, new HttpEntity<String>(hs), String.class, map);

        if (HttpStatus.OK.equals(exchange.getStatusCode())) {
            logger.info(formatGetLog(api, map, exchange));
            return true;
        } else {
            logger.error(formatGetLog(api, map, exchange));
            return false;
        }
    }

    /**
     * Fetch a Flow Execution 获取一个执行流的详细信息 这个流的每个节点的信息 成功或者失败等等
     */
    public boolean fetchFlowInfo(String execId) {
        String api = "Fetch a Flow Execution";
        HttpHeaders hs = getHttpHeadersGet();

        Map<String, String> map = new HashMap<>();
        map.put("id", sessionId);
        map.put("execid", execId);

        //A response sample:
        //{
        //    "attempt" : 0,
        //    "submitUser" : "1",
        //    "updateTime" : 1407779495095,
        //    "status" : "FAILED",
        //    "submitTime" : 1407779473318,
        //    "projectId" : 192,
        //    "flow" : "test",
        //    "endTime" : 1407779495093,
        //    "type" : null,
        //    "nestedId" : "test",
        //    "startTime" : 1407779473354,
        //    "id" : "test",
        //    "project" : "test-azkaban",
        //    "nodes" : [ {
        //    "attempt" : 0,
        //        "startTime" : 1407779495077,
        //        "id" : "test",
        //        "updateTime" : 1407779495077,
        //        "status" : "CANCELLED",
        //        "nestedId" : "test",
        //        "type" : "command",
        //        "endTime" : 1407779495077,
        //        "in" : [ "test-foo" ]
        //}, {
        //    "attempt" : 0,
        //        "startTime" : 1407779473357,
        //        "id" : "test-bar",
        //        "updateTime" : 1407779484241,
        //        "status" : "SUCCEEDED",
        //        "nestedId" : "test-bar",
        //        "type" : "pig",
        //        "endTime" : 1407779484236
        //}, {
        //    "attempt" : 0,
        //        "startTime" : 1407779484240,
        //        "id" : "test-foobar",
        //        "updateTime" : 1407779495073,
        //        "status" : "FAILED",
        //        "nestedId" : "test-foobar",
        //        "type" : "java",
        //        "endTime" : 1407779495068,
        //        "in" : [ "test-bar" ]
        //}, {
        //    "attempt" : 0,
        //        "startTime" : 1407779495069,
        //        "id" : "test-foo",
        //        "updateTime" : 1407779495069,
        //        "status" : "CANCELLED",
        //        "nestedId" : "test-foo",
        //        "type" : "java",
        //        "endTime" : 1407779495069,
        //        "in" : [ "test-foobar" ]
        //} ],
        //    "flowId" : "test",
        //    "execid" : 304
        //}
        ResponseEntity<String> exchange = restTemplate.exchange(
                url + "/executor?session.id={id}&ajax=fetchexecflow&execid={execid}",
                HttpMethod.GET, new HttpEntity<String>(hs), String.class, map);

        if (HttpStatus.OK.equals(exchange.getStatusCode())) {
            logger.info(formatGetLog(api, map, exchange));
            return true;
        } else {
            logger.error(formatGetLog(api, map, exchange));
            return false;
        }
    }

    /**
     * Fetch Execution Job Logs 获取一个执行流的日志
     */
    public boolean fetchFlowLog(String execId, String jobId, String offset, String length) {
        String api = "Fetch Execution Job Logs";
        HttpHeaders hs = getHttpHeadersGet();

        Map<String, String> map = new HashMap<>();
        map.put("id", sessionId);
        map.put("execid", execId);
        map.put("jobId", jobId);
        map.put("offset", offset);
        map.put("length", length);

        //A response sample:
        //{
        //    "data" : "05-08-2014 16:53:02 PDT test-foobar INFO - Starting job test-foobar at 140728278",
        //    "length" : 100,
        //    "offset" : 0
        //}
        ResponseEntity<String> exchange = restTemplate.exchange(
                url + "/executor?session.id={id}&ajax=fetchExecJobLogs&execid={execid}&jobId={jobId}&offset={offset}&length={length}",
                HttpMethod.GET, new HttpEntity<String>(hs), String.class, map);

        if (HttpStatus.OK.equals(exchange.getStatusCode())) {
            logger.info(formatGetLog(api, map, exchange));
            return true;
        } else {
            logger.error(formatGetLog(api, map, exchange));
            return false;
        }
    }

    /**
     * Fetch Flow Execution Updates 获取执行流的信息状态
     */
    public boolean fetchFlowUpdate(String execId) {
        String api = "Fetch Flow Execution Updates";
        HttpHeaders hs = getHttpHeadersGet();

        Map<String, String> map = new HashMap<>();
        map.put("id", sessionId);
        map.put("execid", execId);
        map.put("lastUpdateTime", "-1");

        //A response sample:
        //{
        //    "id" : "test",
        //    "startTime" : 1407778382894,
        //    "attempt" : 0,
        //    "status" : "FAILED",
        //    "updateTime" : 1407778404708,
        //    "nodes" : [ {
        //    "attempt" : 0,
        //        "startTime" : 1407778404683,
        //        "id" : "test",
        //        "updateTime" : 1407778404683,
        //        "status" : "CANCELLED",
        //        "endTime" : 1407778404683
        //}, {
        //    "attempt" : 0,
        //        "startTime" : 1407778382913,
        //        "id" : "test-job-1",
        //        "updateTime" : 1407778393850,
        //        "status" : "SUCCEEDED",
        //        "endTime" : 1407778393845
        //}, {
        //    "attempt" : 0,
        //        "startTime" : 1407778393849,
        //        "id" : "test-job-2",
        //        "updateTime" : 1407778404679,
        //        "status" : "FAILED",
        //        "endTime" : 1407778404675
        //}, {
        //    "attempt" : 0,
        //        "startTime" : 1407778404675,
        //        "id" : "test-job-3",
        //        "updateTime" : 1407778404675,
        //        "status" : "CANCELLED",
        //        "endTime" : 1407778404675
        //} ],
        //    "flow" : "test",
        //    "endTime" : 1407778404705
        //}
        ResponseEntity<String> exchange = restTemplate.exchange(
                url + "/executor?session.id={id}&ajax=fetchexecflowupdate&execid={execid}&lastUpdateTime={lastUpdateTime}",
                HttpMethod.GET, new HttpEntity<String>(hs), String.class, map);

        if (HttpStatus.OK.equals(exchange.getStatusCode())) {
            logger.info(formatGetLog(api, map, exchange));
            return true;
        } else {
            logger.error(formatGetLog(api, map, exchange));
            return false;
        }
    }

    /**
     * Get请求Headers
     * @return HttpHeaders
     */
    private HttpHeaders getHttpHeadersGet() {
        HttpHeaders hs = new HttpHeaders();
        hs.add("Content-Type",
            "application/x-www-form-urlencoded; charset=utf-8");
        hs.add("X-Requested-With", "XMLHttpRequest");
        hs.add("Accept", "text/plain;charset=utf-8");
        return hs;
    }

    /**
     * Post请求Headers
     * @return HttpHeaders
     */
    private HttpHeaders getHttpHeadersPost() {
        HttpHeaders hs = new HttpHeaders();
        hs.add("Content-Type",
            "application/x-www-form-urlencoded; charset=utf-8");
        hs.add("X-Requested-With", "XMLHttpRequest");
        return hs;
    }

    /**
     * 将Post请求结果转换为Json
     * @param result response
     * @return json
     */
    private JsonObject toJson(String result) {
        return new Gson().fromJson(result, JsonObject.class);
    }

    /**
     * 格式化Post请求日志
     * @param api api名称
     * @param httpEntity HttpEntity
     * @param response JsonObject
     * @return 格式化后字符串
     */
    private String formatPostLog(String api, HttpEntity httpEntity, JsonObject response) {
        return String.format(
            "api=%s, request=%s, response=%s", api, httpEntity.toString(), response.toString());
    }

    /**
     * 格式化Get请求人日志
     * @param api api名称
     * @param map 参数Map
     * @return 格式化后字符串
     */
    private String formatGetLog(String api, Map<String, String> map) {
        return String.format("api=%s, map=%s", api, map.toString());
    }

    /**
     * 格式化Get请求人日志
     * @param api api名称
     * @param map 参数Map
     * @param responseEntity ResponseEntity
     * @return 格式化后字符串
     */
    private String formatGetLog(
        String api, Map<String, String> map, ResponseEntity<String> responseEntity) {
        return String.format(
            "api=%s, map=%s, response=%s", api, map.toString(), responseEntity.toString());
    }
}