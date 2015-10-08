package com.hortonworks;

public interface Notifier {
	public void flushMsgs();
	public void shutdown();
}
