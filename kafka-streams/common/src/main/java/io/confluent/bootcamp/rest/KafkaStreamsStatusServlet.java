package io.confluent.bootcamp.rest;

import io.confluent.bootcamp.streams.Context;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.processor.internals.StreamThread;

import java.util.concurrent.ExecutionException;

@Path("/")
public class KafkaStreamsStatusServlet {

    /**
     * Liveness probe for Kubernetes
     */
    @GET
    @Path("alive")
    @Produces(MediaType.APPLICATION_JSON)
    public Response livelinessProbe() throws ExecutionException, InterruptedException {
        var kafkaStreams = Context.getKafkaStreams();
        if (null != kafkaStreams) {
            //Check if KStream is dead
            if (kafkaStreams.state() == KafkaStreams.State.NOT_RUNNING) {
                return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
            }
            return Response.ok().build();
        }
        return Response.status(Response.Status.NO_CONTENT).build();

    }

    /**
     * Readiness probe for Kubernetes
     * Note: It is extremely important to configure properly a readiness probe to ensure that rolling restarts
     * would not be too aggressive and generate outages
     */
    @GET
    @Path("ready")
    @Produces(MediaType.APPLICATION_JSON)
    public Response readinessProbe() throws ExecutionException, InterruptedException {

        var kafkaStreams = Context.getKafkaStreams();
        if (null != kafkaStreams) {
            if (kafkaStreams.state() == KafkaStreams.State.REBALANCING) {
                var starting_thread_count = kafkaStreams.metadataForLocalThreads().stream().filter(t -> StreamThread.State.STARTING.name().compareToIgnoreCase(t.threadState()) == 0 || StreamThread.State.CREATED.name().compareToIgnoreCase(t.threadState()) == 0).count();
                if (((long) kafkaStreams.metadataForLocalThreads().size()) == starting_thread_count) {
                    return Response.status(Response.Status.NO_CONTENT).build();
                }
            }
            return kafkaStreams.state() == KafkaStreams.State.RUNNING || kafkaStreams.state() == KafkaStreams.State.REBALANCING ?
                    Response.ok().build() : Response.status(Response.Status.SERVICE_UNAVAILABLE).build();
        }
        return Response.status(Response.Status.SERVICE_UNAVAILABLE).build();
    }
}

