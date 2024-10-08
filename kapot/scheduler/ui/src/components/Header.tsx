// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import React from "react";
import { Box, Flex, Button } from "@chakra-ui/react";
import Logo from "./logo.svg";
import { HiDocumentText } from "react-icons/hi";
import { AiFillGithub } from "react-icons/ai";
import { SchedulerState } from "./Summary";

export const NavBarContainer: React.FunctionComponent<
  React.PropsWithChildren<any>
> = ({ children, ...props }) => {
  return (
    <Flex
      as="nav"
      align="center"
      justify="space-between"
      wrap="wrap"
      w="100%"
      padding={1}
      bg={["white"]}
      {...props}
    >
      {children}
    </Flex>
  );
};

interface HeaderProps {
  schedulerState?: SchedulerState;
}

export const Header: React.FunctionComponent<HeaderProps> = ({
  schedulerState,
}) => {
  return (
    <NavBarContainer borderBottom={"1px"} borderBottomColor={"#f1f1f1"}>
      <Box w="100%" alignItems={"flex-start"}>
        <NavBarContainer>
          <Flex flexDirection={"row"} alignItems={"center"}>
            <img alt={"kapot Logo"} src={Logo} />
          </Flex>
          <Flex>
            <a
              rel={"noreferrer"}
              target={"_blank"}
              href={"https://datafusion.apache.org/kapot/"}
            >
              <Button
                mr={4}
                colorScheme="blue"
                size="sm"
                rightIcon={<HiDocumentText />}
              >
                Docs
              </Button>
            </a>
            <a
              rel="noreferrer"
              href={"https://github.com/apache/arrow-kapot"}
              target={"_blank"}
            >
              <Button colorScheme="blue" size="sm" rightIcon={<AiFillGithub />}>
                GitHub
              </Button>
            </a>
          </Flex>
        </NavBarContainer>
      </Box>
    </NavBarContainer>
  );
};
