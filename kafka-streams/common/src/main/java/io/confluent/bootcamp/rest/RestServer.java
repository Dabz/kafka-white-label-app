package io.confluent.bootcamp.rest;

import io.confluent.bootcamp.streams.Context;
import org.apache.kafka.streams.state.HostInfo;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.resource.Resource;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestServer implements Runnable {

    static Logger logger = LoggerFactory.getLogger(RestServer.class.getName());
    private Server server;

    @Override
    public void run() {
        HostInfo currentHost = Context.getCurrentHost();
        server = new Server(currentHost.port());

        ServletContextHandler servletCtx = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
        servletCtx.setContextPath("/rest/*");
        HandlerList handlerList = new HandlerList(servletCtx);
        server.setHandler(handlerList);

        ServletHolder serHol = servletCtx.addServlet(ServletContainer.class, "/*");
        serHol.setInitOrder(1);
        serHol.setInitParameter("jersey.config.server.provider.packages", "io.confluent.bootcamp.rest");

        try {
            server.start();
            Runtime.getRuntime().addShutdownHook(new Thread(server::destroy));
        } catch (Exception ex) {
            logger.error(null, ex);
        }
    }

    public Server getServer() {
        return server;
    }
}
