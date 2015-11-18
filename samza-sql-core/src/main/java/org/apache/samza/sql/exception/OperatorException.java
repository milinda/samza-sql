/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samza.sql.exception;

import org.apache.samza.SamzaException;


/**
 * This class defines all exceptions from Samza operator layer
 */
public class OperatorException extends SamzaException {

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  public static enum OperatorError {
    INPUT_DISABLED,
    MESSAGE_TOO_OLD,
    UNKNOWN
  }

  private final OperatorError error;

  private OperatorException(OperatorError error, String errMsg) {
    super(errMsg);
    this.error = error;
  }

  OperatorError getError() {
    return this.error;
  }

  public static OperatorException getInputDisabledException(String errMsg) {
    return new OperatorException(OperatorError.INPUT_DISABLED, errMsg);
  }

  public static OperatorException getMessageTooOldException(String errMsg) {
    return new OperatorException(OperatorError.MESSAGE_TOO_OLD, errMsg);
  }
}