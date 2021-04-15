# Atomic Synchronisation Plugins for Pentaho KETTLE

[![CI](https://github.com/nationalarchives/kettle-atomic-plugins/workflows/CI/badge.svg)](https://github.com/nationalarchives/kettle-atomic-plugins/actions?query=workflow%3ACI)
[![Java 8](https://img.shields.io/badge/java-8+-blue.svg)](https://adoptopenjdk.net/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://opensource.org/licenses/MIT)

This project contains plugins for [Pentaho Data Integration](https://github.com/pentaho/pentaho-kettle) (or KETTLE as it is commonly known),
that add functionality for synchronising steps via Java Atomic values. This enables complex branching workflows where a branch can be conditionally paused
until another branch meets the condition.

Each Java Atomic value is assigned an id, and stored in a "global" concurrent Hash Map. The map is "global" in the sense that it is a
Singleton, and there is only one of them per-JVM.

The plugins provided are:
1. Compare and Set

   <img alt="Compare and Set Icon" src="https://raw.githubusercontent.com/nationalarchives/kettle-atomic-plugins/main/src/main/resources/CompareAndSetStep.svg" width="32"/>
   This flow plugin allows you to optionally initialise (create) an Atomic value with an id, and/or retrieve an Atomic value by id, compare its value, and set it to another value.
   Output to different target steps can be set depending on the current state and/or value of the Atomic value, which enables you to branch your workflow.

2. Await

    <img alt="Await Icon" src="https://raw.githubusercontent.com/nationalarchives/kettle-atomic-plugins/main/src/main/resources/AwaitStep.svg" width="32"/>
    This flow plugin can be used to wait for an Atomic value to become a certain value. This enables you to pause processing a branch of your workflow until a condition is met.

This project was developed by [Evolved Binary](https://evolvedbinary.com) as part of Project OMEGA for the [National Archives](https://nationalarchives.gov.uk).

**NOTE**: When building branching workflows with such synchronisation primitives, great care must be taken to avoid data [Race Conditions](https://en.wikipedia.org/wiki/Race_condition#In_software).

## Getting the Plugins

You can either download the plugins from our GitHub releases page: https://github.com/nationalarchives/kettle-atomic-plugins/releases/, or you can build them from source.

## Building from Source Code
The plugins can be built from Source code by installing the pre-requisites and following the steps described below.

### Pre-requisites for building the project:
* [Apache Maven](https://maven.apache.org/), version 3+
* [Java JDK](https://adoptopenjdk.net/) 1.8
* [Git](https://git-scm.com)

### Build steps:
1. Clone the Git repository
    ```
    $ git clone https://github.com/nationalarchives/kettle-atomic-plugins.git
    ```

2. Compile a package
    ```
    $ cd kettle-atomic-plugins
    $ mvn clean package
    ```
    
3. The plugins directory is then available at `target/kettle-atomic-plugins-1.0.0-SNAPSHOT-kettle-plugin/kettle-atomic-plugins`


## Installing the plugins
* Tested with Pentaho Data Integration - Community Edition - version: 9.1.0.0-324

You need to copy the plugins directory `kettle-atomic-plugins` (from building above) into the `plugins` sub-directory of your KETTLE installation.

This can be done by either running:
```
  $ mvn -Pdeploy-pdi-local -Dpentaho-kettle.plugins.dir=/opt/data-integration/plugins antrun:run@deploy-to-pdi
```

or, you can do so manually, e.g. e.g.:
```
  $ cp -r target/kettle-atomic-plugins-1.0.0-SNAPSHOT-kettle-plugin/kettle-atomic-plugins /opt/data-integration/plugins/
```

## Using the plugins
TODO
