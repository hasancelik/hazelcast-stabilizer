/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.utils;

import org.apache.log4j.Logger;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.Socket;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public final class CommonUtils {

    private static final String EXCEPTION_SEPARATOR = "------ End remote and begin local stack-trace ------";

    private CommonUtils() {
    }

    public static String getSimulatorVersion() {
        String implementationVersion = CommonUtils.class.getPackage().getImplementationVersion();
        return (implementationVersion != null) ? implementationVersion : "SNAPSHOT";
    }

    public static void fixRemoteStackTrace(Throwable remoteCause, StackTraceElement[] localSideStackTrace) {
        StackTraceElement[] remoteStackTrace = remoteCause.getStackTrace();
        StackTraceElement[] newStackTrace = new StackTraceElement[localSideStackTrace.length + remoteStackTrace.length];
        System.arraycopy(remoteStackTrace, 0, newStackTrace, 0, remoteStackTrace.length);
        newStackTrace[remoteStackTrace.length] = new StackTraceElement(EXCEPTION_SEPARATOR, "", null, -1);
        System.arraycopy(localSideStackTrace, 1, newStackTrace, remoteStackTrace.length + 1, localSideStackTrace.length - 1);
        remoteCause.setStackTrace(newStackTrace);
    }

    public static RuntimeException rethrow(Throwable throwable) {
        if (throwable instanceof RuntimeException) {
            throw (RuntimeException) throwable;
        } else {
            throw new CommandLineExitException(throwable);
        }
    }

    public static String throwableToString(Throwable throwable) {
        StringWriter stringWriter = new StringWriter();
        throwable.printStackTrace(new PrintWriter(stringWriter));
        return stringWriter.toString();
    }

    public static void closeQuietly(Socket socket) {
        if (socket == null) {
            return;
        }
        try {
            socket.close();
        } catch (IOException ignore) {
            EmptyStatement.ignore(ignore);
        }
    }

    public static void closeQuietly(Closeable... closeables) {
        for (Closeable c : closeables) {
            closeQuietly(c);
        }
    }

    public static void closeQuietly(Closeable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (IOException ignore) {
            EmptyStatement.ignore(ignore);
        }
    }

    public static void closeQuietly(XMLStreamWriter writer) {
        if (writer == null) {
            return;
        }
        try {
            writer.close();
        } catch (XMLStreamException ignore) {
            EmptyStatement.ignore(ignore);
        }
    }

    public static void joinThread(Thread thread) {
        try {
            thread.join();
        } catch (InterruptedException e) {
            EmptyStatement.ignore(e);
            Thread.currentThread().interrupt();
        }
    }

    public static void sleepSeconds(int seconds) {
        try {
            TimeUnit.SECONDS.sleep(seconds);
        } catch (InterruptedException ignore) {
            EmptyStatement.ignore(ignore);
        }
    }

    public static void sleepMillis(int millis) {
        try {
            TimeUnit.MILLISECONDS.sleep(millis);
        } catch (InterruptedException ignore) {
            EmptyStatement.ignore(ignore);
        }
    }

    public static void sleepNanos(long nanos) {
        if (nanos <= 0) {
            return;
        }
        LockSupport.parkNanos(nanos);
    }

    public static void sleepSecondsThrowException(int seconds) {
        try {
            TimeUnit.SECONDS.sleep(seconds);
        } catch (InterruptedException e) {
            throw rethrow(e);
        }
    }

    public static void sleepMillisThrowException(int millis) {
        try {
            TimeUnit.MILLISECONDS.sleep(millis);
        } catch (InterruptedException e) {
            throw rethrow(e);
        }
    }

    /**
     * Sleeps a random amount of time.
     *
     * The call is ignored if maxDelayNanos equals or smaller than zero.
     *
     * @param random        the Random used to randomize
     * @param maxDelayNanos the maximum sleeping period in nanoseconds
     */
    public static void sleepRandomNanos(Random random, long maxDelayNanos) {
        if (maxDelayNanos <= 0) {
            return;
        }
        long randomValue = Math.abs(random.nextLong() + 1);
        sleepNanos(randomValue % maxDelayNanos);
    }

    public static long getElapsedSeconds(long started) {
        return NANOSECONDS.toSeconds(System.nanoTime() - started);
    }

    public static void exitWithError() {
        System.exit(1);
    }

    public static void exitWithError(Logger logger, String msg, Throwable throwable) {
        if (throwable instanceof CommandLineExitException) {
            String logMessage = throwable.getMessage();
            if (throwable.getCause() != null) {
                String throwableString = throwableToString(throwable.getCause());
                logMessage += NEW_LINE + throwableString;
            }
            logger.fatal(logMessage);
        } else {
            String throwableString = throwableToString(throwable);
            logger.fatal(msg + NEW_LINE + throwableString);
        }
        System.exit(1);
    }
}
