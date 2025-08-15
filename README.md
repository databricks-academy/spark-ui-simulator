# Spark UI Simulator - Databricks Academy

A comprehensive educational tool for learning Apache Spark concepts through interactive simulations.

**Important Notice**: This simulator represents a snapshot in time of the Spark UI and is intended solely for instructional purposes. It does not demonstrate the latest Spark features or UI improvements, but serves as a historical reference for learning core Spark concepts.

## Purpose

This application was developed to aid in the instruction of Apache Spark concepts. Some of these concepts require an inordinate amount of time to demonstrate, making it impractical to do so live or otherwise costly for you to do so on your own time.

For this reason, pre-run jobs, all the metrics related to the execution of those jobs, the cluster configuration, and the source code for those jobs are bundled with each simulation. You are encouraged to reproduce these scenarios to deepen your understanding of Apache Spark.

## Features

- **Pre-recorded Spark Jobs**: Complete execution data from real Spark applications
- **Interactive UI Elements**: Navigate through relevant parts of the Spark UI
- **Source Code Included**: Both Python and Scala implementations available
- **Cluster Configuration**: View the exact cluster settings used for each experiment
- **Educational Focus**: Designed specifically for learning Spark performance concepts

## Structure

The simulator contains multiple experiments organized in folders:

- `experiment-XXXX/` - Individual simulation scenarios
- `common-ui/` - Shared UI components and styles
- `common-screenshots/` - Reusable interface elements
- Each experiment includes:
  - HTML interface files
  - Source code (Python `.py` or Scala `.scala`)
  - Screenshots of the actual Spark UI
  - Configuration files

## Limitations

As a simulator, there are specific, known limitations:

- **Limited Interactivity**: Not every part of the screen is interactive - only those elements that are deemed relevant to a simulation are enabled
- **Static Content**: The screen is composed of screenshots making it impossible to grep for text the way you would with a live application
- **Timestamp Accuracy**: Timestamps across a single simulation will be inaccurate - this is a byproduct of having to rerun experiments multiple times to capture different content

## Getting Started

1. Open `index.html` in your web browser to access the main simulation menu
2. Navigate to any experiment folder to explore specific scenarios
3. Each experiment contains its own `index.html` file with the simulation interface
4. Review the included source code to understand the Spark jobs being simulated

## Educational Use

This simulator is ideal for:
- Understanding Spark UI navigation
- Learning performance optimization concepts
- Analyzing job execution patterns
- Studying different Spark application behaviors