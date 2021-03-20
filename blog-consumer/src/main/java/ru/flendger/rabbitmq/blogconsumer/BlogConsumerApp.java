package ru.flendger.rabbitmq.blogconsumer;

import com.rabbitmq.client.*;

import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class BlogConsumerApp {
    private static final String EXCHANGE_NAME = "blogExchanger";
    private static volatile String topic = "";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(" [x] Received '" + message + "'");
        };

        try (Scanner scanner = new Scanner(System.in);
             Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            String queueName = channel.queueDeclare().getQueue();
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

            String cmd;
            System.out.println("Enter topic name first");
            do {
                System.out.print(">>> ");
                cmd = scanner.nextLine();
                if (cmd.equals("/exit")) return;

                String[] strings = cmd.split(" ", 2);
                if (strings.length != 2 || !strings[0].equals("set_topic")) {
                    System.out.println("Type [set_topic <topic name>] for setting topic name");
                    continue;
                }
                channel.queueUnbind(queueName, EXCHANGE_NAME, topic);
                topic = strings[1];
                System.out.println("Started listening messages for topic: " + topic);

                channel.queueBind(queueName, EXCHANGE_NAME, topic);
                channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
                });
            } while (true);
        }
    }
}
