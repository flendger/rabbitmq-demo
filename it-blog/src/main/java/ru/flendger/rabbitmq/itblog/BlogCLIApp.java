package ru.flendger.rabbitmq.itblog;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class BlogCLIApp {
    private static final String EXCHANGE_NAME = "blogExchanger";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel();
             Scanner scanner = new Scanner(System.in)) {
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

            String cmd;
            String topic = "";
            System.out.println("Enter topic name first");
            do {
                System.out.print(">>> ");
                cmd = scanner.nextLine();
                if (cmd.equals("/exit")) break;

                if (topic.isEmpty()) {
                    String[] strings = cmd.split(" ", 2);
                    if (strings.length != 2 || !strings[0].equals("set_topic")) {
                        System.out.println("Type [set_topic <topic name>] for setting topic name");
                        continue;
                    }
                    topic = strings[1];
                    System.out.println("Topic name is set for " + topic);
                    continue;
                }

                channel.basicPublish(EXCHANGE_NAME, topic, null, cmd.getBytes(StandardCharsets.UTF_8));
                System.out.println(" [x] Sent '" + cmd + "'");
            } while (true);
        }
    }
}
