package de.olfdb.hivemqclient;

import java.util.UUID;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import com.hivemq.client.mqtt.mqtt3.Mqtt3AsyncClient;
import com.hivemq.client.mqtt.mqtt3.Mqtt3Client;

public class HiveMQClient {

    boolean connected = false;
    String server = "tcp://localhost:1883";
    String topic = "navit/traff";
    boolean compressed = false;

    public static void main(String[] args) {

        HiveMQClient me = new HiveMQClient();

        if (args.length > 0) {
            me.server = args[0];
        }

        if (args.length > 1) {
            me.topic = args[1];
        }

        if (args.length > 2) {
            me.compressed = args[2].equals("-c") ? true : false;
        }

        final Mqtt3AsyncClient client = Mqtt3Client.builder().identifier(UUID.randomUUID().toString())
                .serverHost(me.server).buildAsync();

        while (true) {
            try {
                if (!me.connected) {
                    client.connect().whenComplete((connAck, throwable) -> {
                        if (throwable != null) {
                            // Handle connection failure
                            System.out.println("Failed to connect to " + me.server + "!");
                            me.connected = false;
                        } else {
                            me.connected = true;
                            client.subscribeWith().topicFilter(me.topic).callback(publish -> {
                                // Process the received message
                                if (me.compressed) {
                                    Inflater decompressor = new Inflater();
                                    try {
                                        decompressor.setInput(publish.getPayloadAsBytes());
                                        byte[] uncompressed = new byte[4096];
                                        decompressor.inflate(uncompressed);
                                        System.out.println("Compressed message received:\n" + new String(uncompressed));
                                    } catch (DataFormatException e) {
                                        // TODO Auto-generated catch block
                                        e.printStackTrace();
                                    }
                                } else { //uncompressed message
                                    System.out.println("Plain message received:\n" + new String(publish.getPayloadAsBytes()));
                                }
                            }).send().whenComplete((subAck, throwable1) -> {
                                if (throwable1 != null) {
                                    // Handle failure to subscribe
                                    System.out.println("Failed to subscribe to " + me.topic + " at localhost!");
                                    throwable1.printStackTrace();
                                } else {
                                    // Handle successful subscription, e.g. logging or incrementing a metric
                                    System.out.println("Subscribed to " + me.topic + " at " + me.server);
                                }
                            });
                        }
                    });
                }
            } catch (Exception e) {
e.printStackTrace();
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }
}
