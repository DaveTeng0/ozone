# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

*** Settings ***
Documentation       Ozone FS tests
Library             OperatingSystem
Resource            ../commonlib.robot
Resource            setup.robot
Test Timeout        5 minutes
Suite Setup         Setup for FS test

*** Test Cases ***
Check disk usage after create a file which uses RATIS replication type
                   #Execute               ozone sh volume create /vol1
                   Execute               ozone sh bucket create /vol1/bucket4 --replication 3 --type RATIS
                   Execute And Ignore Error              ozone fs -put NOTICE.txt ofs://om/vol1/bucket4/PUTFILE.txt
                   ${expectedFileLength} =    Execute      stat -c%s "NOTICE.txt"     
                   ${expectedDiskUsage} =     Evaluate     3 * ${expectedFileLength}
                   ${expectedDiskUsage} =     Convert to String  ${expectedDiskUsage}
                   ${result} =    Execute               ozone fs -du ofs://om/vol1/bucket4
                                    Should contain   ${result}   PUTFILE.txt
                                    Should contain   ${result}   ${expectedFileLength}
                                    Should contain   ${result}   ${expectedDiskUsage}

Check disk usage after create a file which uses EC replication type
                   #Execute               ozone sh volume create /vol1
                   # Execute               ozone sh bucket create /vol1/bucket3 --type EC --replication rs-3-2-1024k
                   Execute And Ignore Error              ozone fs -put NOTICE.txt ofs://om/vol1/bucket3/PUTFILE.txt
                   ${expectedFileLength} =    Execute      stat -c%s "NOTICE.txt"     
                   ${dataStripeSize} =    Evaluate   3 * 1024 * 1024
                   ${fullStripes} =    Evaluate   ${expectedFileLength}/${dataStripeSize} 
                   ${fullStripes} =    Convert To Integer   ${fullStripes}
                   ${fullStripes} =    Convert to Number    ${fullStripes} 0
                   ${ecChunkSize} =    Evaluate   1024 * 1024
                   ${partialFirstChunk} =      Evaluate   ${expectedFileLength} % ${dataStripeSize}

                   ${ecChunkSize} =   Convert To Integer   ${ecChunkSize}
                   ${partialFirstChunk} =   Convert To Integer   ${partialFirstChunk}


                   ${partialFirstChunkOptions} =    Create List   ${ecChunkSize}   ${partialFirstChunk}
                   ${partialFirstChunk} =      Evaluate   min(${partialFirstChunkOptions})
                   ${replicationOverhead} =    Evaluate   ${fullStripes} * 2 * 1024 * 1024 + ${partialFirstChunk} * 2
                   ${expectedDiskUsage} =      Evaluate   ${expectedFileLength} + ${replicationOverhead}  
                   ${expectedDiskUsage} =      Convert To Integer    ${expectedDiskUsage} 
                   ${result} =    Execute               ozone fs -du ofs://om/vol1/bucket3 
                                    Should contain        ${result}         PUTFILE.txt
                                    Should contain        ${result}         ${expectedFileLength}
                                    ${expectedDiskUsage} =        Convert To String    ${expectedDiskUsage}
                                    Should contain        ${result}         ${expectedDiskUsage}





#Check disk usage after create a file which uses RATIS replication type
#                ${vol} =    BuiltIn.Set Variable    /vol1
#               ${bucket} =  BuiltIn.Set Variable    /bucket1
#                            Execute And Ignore Error               ozone sh volume create ${vol}
#                            Execute And Ignore Error               ozone sh bucket create ${vol}${bucket} --replication 3 --type RATIS
#                            Execute And Ignore Error               ozone fs -put NOTICE.txt ${vol}${bucket}/PUTFILE1.txt
##                            Execute               ozone sh volume create ${vol}
##                            Execute               ozone sh bucket create ${vol}${bucket} --replication 3 --type RATIS
##                            Execute               ozone fs -put NOTICE.txt ${vol}${bucket}/PUTFILE1.txt
#     ${expectedFileLength} =    Execute               stat -c %s NOTICE.txt
#                                Log To Console        fileLength=${expectedFileLength}
#     ${expectedDiskUsage} =     Get Disk Usage of File with RATIS Replication    ${expectedFileLength}    3
#     ${result} =                Execute               ozone fs -du ofs://om${vol}${bucket}
#                                Should contain        ${result}         PUTFILE1.txt
#                                Log To Console        reuslt=${result}
#                                Should contain        ${result}         ${expectedFileLength}
#                                Should contain        ${result}         ${expectedDiskUsage}



#Check disk usage after create a file which uses EC replication type
#                   ${vol} =    BuiltIn.Set Variable    /vol2
#                   ${bucket} =    BuiltIn.Set Variable    /bucket2
#
#                   Execute And Ignore Error               ozone sh volume create ${vol}
#                   Execute And Ignore Error               ozone sh bucket create ${vol}${bucket} --type EC --replication rs-3-2-1024k
##                  Execute                ozone sh volume create ${vol}
##                  Execute                ozone sh bucket create ${vol}${bucket} --type EC --replication rs-3-2-1024k
#
#                   Execute And Ignore Error               ozone fs -put NOTICE.txt ${vol}${bucket}/PUTFILE2.txt
#                   ${expectedFileLength} =    Execute      stat -c %s NOTICE.txt
#                   Log To Console        fileLength=${expectedFileLength}
#                   ${expectedDiskUsage} =     Get Disk Usage of File with EC RS Replication    ${expectedFileLength}    3    2    1024
#                   Log To Console     du=${expectedDiskUsage}
#                   ${result} =    Execute               ozone fs -du ofs://om${vol}${bucket}
#                            Should contain        ${result}         PUTFILE2.txt
#                            Log To Console        reuslt=${result}
#                            Should contain        ${result}         ${expectedFileLength}
#                            Should contain        ${result}         ${expectedDiskUsage}



*** Keywords ***

Setup localdir1
                   Execute               rm -Rf /tmp/localdir1
                   Execute               mkdir /tmp/localdir1
                   Execute               cp NOTICE.txt /tmp/localdir1/LOCAL.txt
                   Execute               ozone fs -mkdir -p ${BASE_URL}testdir1
                   Execute               ozone fs -copyFromLocal /tmp/localdir1 ${BASE_URL}testdir1/
                   Execute               ozone fs -put NOTICE.txt ${BASE_URL}testdir1/NOTICE.txt
