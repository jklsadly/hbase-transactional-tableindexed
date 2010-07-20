/**
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver.transactional;

import java.io.IOException;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ipc.TransactionalRegionInterface;
import org.apache.hadoop.hbase.regionserver.DisabledTestRegionServerExit;

/**
 * Tests unexpected exit of region server.
 */
public class TestRegionServerExit extends DisabledTestRegionServerExit {

    public TestRegionServerExit() {
        super();
        conf.set(HConstants.REGION_SERVER_CLASS, TransactionalRegionInterface.class.getName());
        conf.set(HConstants.REGION_SERVER_IMPL, TransactionalRegionServer.class.getName());
    }

    @Override
    public void testAbort() throws IOException {
        super.testAbort();
    }

}
