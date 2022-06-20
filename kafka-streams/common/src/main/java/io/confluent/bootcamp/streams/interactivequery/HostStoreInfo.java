package io.confluent.bootcamp.streams.interactivequery;

import io.confluent.bootcamp.streams.Context;
import org.apache.kafka.streams.state.HostInfo;

import java.util.Objects;
import java.util.Set;

public class HostStoreInfo {

  private String host;
  private int port;
  private Set<String> storeNames;

  public HostStoreInfo() {

  }

  public HostStoreInfo(final String host, final int port, final Set<String> storeNames) {
    this.host = host;
    this.port = port;
    this.storeNames = storeNames;
  }

  public String getHost() {
    return host;
  }

  public void setHost(final String host) {
    this.host = host;
  }

  public int getPort() {
    return port;
  }

  public void setPort(final int port) {
    this.port = port;
  }

  public Set<String> getStoreNames() {
    return storeNames;
  }

  public void setStoreNames(final Set<String> storeNames) {
    this.storeNames = storeNames;
  }

  @Override
  public String toString() {
    return "HostStoreInfo{" +
           "host='" + host + '\'' +
           ", port=" + port +
           ", storeNames=" + storeNames +
           '}';
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final HostStoreInfo that = (HostStoreInfo) o;
    return port == that.port &&
           Objects.equals(host, that.host) &&
           Objects.equals(storeNames, that.storeNames);
  }

  public boolean thisHost(HostInfo host) {
    return this.host.equals(host.host()) && (this.port == host.port());
  }

  public boolean thisHost() {
    var host = Context.getCurrentHost();
    return this.host.equals(host.host()) && (this.port == host.port());
  }

  @Override
  public int hashCode() {
    return Objects.hash(host, port, storeNames);
  }
}