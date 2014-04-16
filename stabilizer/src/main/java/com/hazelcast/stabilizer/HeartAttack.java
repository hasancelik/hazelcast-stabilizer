/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.stabilizer;

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.util.Date;

public class HeartAttack implements Serializable {

    private static final long serialVersionUID = 1;

    private final String message;
    private final InetSocketAddress coachAddress;
    private final InetSocketAddress traineeAddress;
    private final String traineeId;
    private final Date time;
    private final ExerciseRecipe exerciseRecipe;
    private final Throwable cause;

    public HeartAttack(String message, InetSocketAddress coachAddress, InetSocketAddress traineeAddress,
                       String traineeId, ExerciseRecipe exerciseRecipe) {
        this.message = message;
        this.coachAddress = coachAddress;
        this.traineeId = traineeId;
        this.time = new Date();
        this.exerciseRecipe = exerciseRecipe;
        this.traineeAddress = traineeAddress;
        this.cause = null;
    }

    public HeartAttack(String message, InetSocketAddress coachAddress, InetSocketAddress traineeAddress,
                       String traineeId, ExerciseRecipe exerciseRecipe, Throwable cause) {
        this.message = message;
        this.coachAddress = coachAddress;
        this.traineeId = traineeId;
        this.time = new Date();
        this.exerciseRecipe = exerciseRecipe;
        this.traineeAddress = traineeAddress;
        this.cause = cause;
    }

    public Throwable getCause() {
        return cause;
    }

    public String getMessage() {
        return message;
    }

    public String getTraineeId() {
        return traineeId;
    }

    public InetSocketAddress getCoachAddress() {
        return coachAddress;
    }

    public Date getTime() {
        return time;
    }

    public ExerciseRecipe getExerciseRecipe() {
        return exerciseRecipe;
    }

    public InetSocketAddress getTraineeAddress() {
        return traineeAddress;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("HeartAttack[\n");
        sb.append("   message='").append(message).append("'\n");
        sb.append("   coachAddress=").append(coachAddress).append("\n");
        sb.append("   time=").append(time).append("\n");
        sb.append("   traineeAddress=").append(traineeAddress).append("\n");
        sb.append("   traineeId=").append(traineeId).append("\n");
        if (exerciseRecipe != null) {
            String[] exerciseString = exerciseRecipe.toString().split("\n");
            sb.append("   exercise=").append(exerciseString[0]).append("\n");
            for (int k = 1; k < exerciseString.length; k++) {
                sb.append("    ").append(exerciseString[k]).append("\n");
            }
        } else {
            sb.append("   exercise=").append("null").append("\n");
        }

        if (cause != null) {
            StringWriter sw = new StringWriter();
            cause.printStackTrace(new PrintWriter(sw));
            sb.append("   cause=").append(sw.toString()).append("\n");
        } else {
            sb.append("   cause=").append("null").append("\n");
        }

        sb.append("]");
        return sb.toString();
    }
}
