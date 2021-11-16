/*
 * Copyright © 2021 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.internal.app.runtime.monitor;

import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.proto.ProgramRunStatus;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Used to send information about program status from RuntimeHandler to RuntimeClient.
 */
public class RunRecordStatus {
  private final ProgramRunStatus programRunStatus;
  @Nullable
  private final String message;

  private RunRecordStatus(ProgramRunStatus programRunStatus, String message) {
    this.programRunStatus = programRunStatus;
    this.message = message;
  }

  public ProgramRunStatus getProgramRunStatus() {
    return programRunStatus;
  }

  @Nullable
  public String getMessage() {
    return message;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    RunRecordStatus that = (RunRecordStatus) o;
    return programRunStatus.equals(that.programRunStatus) &&
      message.equals(that.message);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getProgramRunStatus(), getMessage());
  }

  @Override
  public String toString() {
    return com.google.common.base.Objects.toStringHelper(this)
      .add("programRunStatus", getProgramRunStatus())
      .add("message", getMessage())
      .toString();
  }

  /**
   * Get a builder for creating a record with the given program status and message.
   *
   * @return builder for creating a record with the given schema.
   * @throws UnexpectedFormatException if the given schema is not a record with at least one field.
   */
  public static Builder builder() throws UnexpectedFormatException {
    return new Builder();
  }

  /**
   * Builder for creating a {@link RunRecordStatus}.
   */
  public static class Builder {
    private ProgramRunStatus programRunStatus;
    private String message;

    private Builder() {
    }

    public Builder setProgramRunStatus(ProgramRunStatus programRunStatus) {
      this.programRunStatus = programRunStatus;
      return this;
    }

    public Builder setMessage(String message) {
      this.message = message;
      return this;
    }

    public RunRecordStatus build() {
      if (programRunStatus == null) {
        throw new IllegalArgumentException("Program run status must be specified.");
      }
      // we are not validating message for null,
      // message could be null for program run status other than for STOPPING where we might not want
      // to send any other data apart from the status itself
      return new RunRecordStatus(programRunStatus, message);
    }
  }
}
