/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.iceberg

object IcebergUtils {
def getGenericDataFilePath(data: StructLike): String = {
    data.asInstanceOf[GenericDataFile].path().toString
  }
}
