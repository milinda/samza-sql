/**
 * Copyright 2016 Milinda Pathirage
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.runners.samza.utils;

public class Validations {
  public static void isNullOrEmpty(String value, String kind) {
    isNull(value, kind);
    isEmpty(value, kind);
  }

  private static void isEmpty(String value, String kind) {
    if (value.isEmpty()) {
      throw new IllegalArgumentException(kind + " is empty.");
    }
  }

  public static void isNull(Object value, String kind) {
    if(value == null) {
      throw new IllegalArgumentException(kind + " is null.");
    }
  }

  public static boolean isNullOrEmpty(String value){
    return value == null || value.isEmpty();
  }
}
