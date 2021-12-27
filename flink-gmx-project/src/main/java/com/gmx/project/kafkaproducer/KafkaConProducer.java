package com.gmx.project.kafkaproducer;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KafkaConProducer {
    //发送消息个数
    private static final int MSG_SIZE = 100;
    private static Long COUNT = 0L;
    //负责发送消息的线程池,根据系统资源进行设置
    private static ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    private static CountDownLatch countDownLatch = new CountDownLatch(Integer.MAX_VALUE);

    private static class ProducerWorker implements Runnable {
        private ProducerRecord<String, String> record;
        private KafkaProducer<String, String> producer;

        public ProducerWorker(ProducerRecord<String, String> record, KafkaProducer<String, String> producer) {
            this.record = record;
            this.producer = producer;
        }

        @Override
        public void run() {
            final String id = Thread.currentThread().getId() + "-" + System.identityHashCode(producer);
            try {
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            e.printStackTrace();
                        }
                        if (recordMetadata != null) {
                            System.out.println("回调：offset:" + recordMetadata.offset() + ";partition:" + recordMetadata.partition());
                        }
                    }
                });
//                System.out.println(COUNT++ + " " + id + "：数据已发送。");
                countDownLatch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        String host = "";
        String port = "";
        String topic = "";
        String message = "{\"sync_time\":\"1631164340551\",\"agent_id\":\"ab969365-720a-48d8-9f4b-e13800095c4e\",\"agent_time\":\"1631164248495\",\"event_time\":\"1631164247024\",\"computer\":\"DC02.test16.local\",\"domain\":\"TEST16.LOCAL\",\"event_id\":4624,\"event_record_id\":\"141785751\",\"channel\":\"Security\",\"command_line\":null,\"execution_process_id\":660,\"execution_thread_id\":4248,\"log_level\":\"信息\",\"mandatory_label\":null,\"message\":\"已成功登录帐户。\\n\\n使用者:\\n\\t安全 ID:\\t\\tS-1-0-0\\n\\t帐户名称:\\t\\t-\\n\\t帐户域:\\t\\t-\\n\\t登录 ID:\\t\\t0x0\\n\\n登录信息:\\n\\t登录类型:\\t\\t3\\n\\t受限制的管理员模式:\\t-\\n\\t虚拟帐户:\\t\\t否\\n\\t提升的令牌:\\t\\t是\\n\\n模拟级别:\\t\\t模拟\\n\\n新登录:\\n\\t安全 ID:\\t\\tS-1-5-18\\n\\t帐户名称:\\t\\tDC02$\\n\\t帐户域:\\t\\tTEST16.LOCAL\\n\\t登录 ID:\\t\\t0xEF16F61\\n\\t链接的登录 ID:\\t\\t0x0\\n\\t网络帐户名称:\\t-\\n\\t网络帐户域:\\t-\\n\\t登录 GUID:\\t\\t{1E0D938F-8609-9C5B-8C9E-BE513026ABBF}\\n\\n进程信息:\\n\\t进程 ID:\\t\\t0x0\\n\\t进程名称:\\t\\t-\\n\\n网络信息:\\n\\t工作站名称:\\t-\\n\\t源网络地址:\\tfe80::ac26:4795:5811:aa67\\n\\t源端口:\\t\\t65394\\n\\n详细的身份验证信息:\\n\\t登录进程:\\t\\tKerberos\\n\\t身份验证数据包:\\tKerberos\\n\\t传递的服务:\\t-\\n\\t数据包名(仅限 NTLM):\\t-\\n\\t密钥长度:\\t\\t0\\n\\n创建登录会话时，将在被访问的计算机上生成此事件。\\n\\n“使用者”字段指示本地系统上请求登录的帐户。这通常是一个服务(例如 Server 服务)或本地进程(例如 Winlogon.exe 或 Services.exe)。\\n\\n“登录类型”字段指示发生的登录类型。最常见的类型是 2 (交互式)和 3 (网络)。\\n\\n“新登录”字段指示新登录是为哪个帐户创建的，即已登录的帐户。\\n\\n“网络”字段指示远程登录请求源自哪里。“工作站名称”并非始终可用，并且在某些情况下可能会留空。\\n\\n“模拟级别”字段指示登录会话中的进程可以模拟到的程度。\\n\\n“身份验证信息”字段提供有关此特定登录请求的详细信息。\\n\\t- “登录 GUID”是可用于将此事件与 KDC 事件关联起来的唯一标识符。\\n\\t-“传递的服务”指示哪些中间服务参与了此登录请求。\\n\\t-“数据包名”指示在 NTLM 协议中使用了哪些子协议。\\n\\t-“密钥长度”指示生成的会话密钥的长度。如果没有请求会话密钥，则此字段将为 0。\",\"provider_name\":\"Microsoft-Windows-Security-Auditing\",\"provider_guid\":\"{54849625-5478-4994-A5BA-3E3B0328C30D}\",\"access_list\":null,\"access_mask\":null,\"access_reason\":null,\"account_name\":null,\"allowed_to_delegate_to\":null,\"app_correlation_id\":null,\"attribute_ldap_display_name\":null,\"attribute_syntax_oid\":null,\"attribute_value\":null,\"audit_source_name\":null,\"authentication_package_name\":\"Kerberos\",\"elevated_token\":\"%%1842\",\"ds_name\":null,\"ds_type\":null,\"ip_address\":\"fe80::ac26:4795:5811:aa67\",\"ip_port\":null,\"image_path\":null,\"impersonation_level\":\"%%1833\",\"key_length\":\"0\",\"lm_package_name\":\"-\",\"logon_guid\":\"{1E0D938F-8609-9C5B-8C9E-BE513026ABBF}\",\"logon_process_name\":\"Kerberos\",\"logon_type\":\"3\",\"member_name\":null,\"member_sid\":null,\"new_process_id\":null,\"new_process_name\":null,\"new_uac_value\":null,\"object_class\":null,\"object_dn\":null,\"object_guid\":null,\"object_name\":null,\"object_server\":null,\"object_type\":null,\"old_uac_value\":null,\"op_correlation_id\":null,\"operation_type\":null,\"param1\":null,\"param2\":null,\"param3\":null,\"param4\":null,\"param5\":null,\"param6\":null,\"param7\":null,\"param8\":null,\"param9\":null,\"param10\":null,\"param11\":null,\"param12\":null,\"param13\":null,\"param14\":null,\"param15\":null,\"param16\":null,\"parent_process_name\":null,\"password_last_set\":null,\"privilege_list\":null,\"process_id\":null,\"process_name\":\"-\",\"properties\":null,\"restricted_admin_mode\":\"-\",\"relative_target_name\":null,\"service_account\":null,\"service_file_name\":null,\"service_name\":null,\"service_principal_names\":null,\"service_start_type\":null,\"service_sid\":null,\"service_type\":null,\"share_name\":null,\"share_local_path\":null,\"sid_history\":null,\"source_dra\":null,\"start_type\":null,\"status\":null,\"subject_domain_name\":\"-\",\"subject_logon_id\":\"0x0\",\"subject_user_sid\":\"S-1-0-0\",\"subject_username\":\"-\",\"task_name\":null,\"task_content\":null,\"target_domain_name\":\"TEST16.LOCAL\",\"target_info\":null,\"target_linked_logon_id\":\"0x0\",\"target_logon_id\":\"0xef16f61\",\"target_logon_guid\":null,\"target_outbound_domain_name\":\"-\",\"target_outbound_username\":\"-\",\"target_server_name\":null,\"target_sid\":null,\"target_user_sid\":\"S-1-5-18\",\"target_username\":\"DC02$\",\"ticket_encryption_type\":null,\"ticket_options\":null,\"token_elevation_type\":null,\"transmitted_services\":\"-\",\"user_identifier\":null,\"virtual_account\":\"%%1843\",\"workstation\":null,\"workstation_name\":\"-\"}";
        String quantity = "";

        if (args.length == 0) {
            throw new IllegalArgumentException("args: java -jar xxx.jar -host xx -port xx -topic xx  -quantity xx");
        } else {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            host = parameterTool.get("host");
            port = parameterTool.get("port");
            topic = parameterTool.get("topic");
//            message = parameterTool.get("message");
            quantity = parameterTool.get("quantity");
        }
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, host + ":" + port);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, "0");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432"); //max_insert_block_size 1048545
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        try {
            for (int i = 0; i < Integer.parseInt(quantity); i++) {
                ProducerRecord<String, String> record = new ProducerRecord<String, String>(
                        topic, null, System.currentTimeMillis(), String.valueOf(i), message);
                executorService.submit(new ProducerWorker(record, producer));
                countDownLatch.await();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            producer.close();
            executorService.shutdown();
        }
    }
}
