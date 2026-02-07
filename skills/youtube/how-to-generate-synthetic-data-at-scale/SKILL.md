---
name: how-to-generate-synthetic-data-at-scale
description: #
---

# How to generate synthetic data at scale

## Overview
#

## Video Information
- **Author:** Canadian Data Guy
- **Date:** 2025-01-03
- **URL:** https://www.youtube.com/watch?v=UsOqtW3Nebw

## Tags
data-engineering

## Key Topics Covered
- Spark Streaming and Delta Lake concepts
- Practical implementation guidance
- Production best practices

## Content Summary

  
**Video ID:** UsOqtW3Nebw

### Summary

#### **TL;DR**
- Efficiently generate synthetic IoT data using Spark with a custom library for under $0.45 per terabyte, ideal for privacy concerns or performance testing.

#### **Key Ideas**
1. **Why Use Synthetic Data?**
   - Privacy and compliance hurdles.
   - Testing application limits without relying on real data.
   - Accelerate development by decoupling from upstream data sources.

2. **Methodology**
   - **Hardware:** Utilizes a 4-core machine with 32GB RAM for efficient processing.
   - **Tools:** Uses Spark Parquet libraries and the DLB Data Gen library to create constrained IoT datasets.
   - **Schema Specification:** Generates structured data with defined parameters (e.g., temperature ranges, device IDs).

3. **Performance**
   - Achieves 1 million rows per second on a 4-core setup, scaling up to four million rows per second.

4. **Cost Example**
   - Real-world cost: Approximately $0.25 for generating terabytes of data.

### Full Transcript

Hey folks, welcome to 2025, I'm Soni, also known as the Canadian Data Guy. Starting now, all of our blogs are going to have an accompanying video, and I'm committed to publishing a video every other week. Let's explore our latest blog, which is about how to generate a terabyte of synthetic data in less than a dollar.

We are going to generate IoT data, but you can create any data set you like. I'm going to leverage a four core machine, and each row is going to be about a kilobyte of data. The speed which we are aiming for is one million rows a second. You can go higher. So, with four cores, I'm hoping for a million rows a second. You make it eight cores, you'll probably get two million. You go to 16 cores, you'll probably get four million.

Two billion rows, roughly constitutes 2000 gigabytes or a terabyte. If you produce one million rows a second, it would take approximately a thousand seconds Within 16 minutes, with four cores, you'll be done with a billion rows.

First thing first, why do we would create synthetic data sets? A couple of reasons. First, as you can think of as privacy and compliance, dev workspace can't have broad data, right? That's one of the reasons, which is if you are in healthcare, you are in finance, a lot of times you don't have the ability to have the data set, which you would like in a dev environment.

Other reason, which is my reason, which is why I'm doing it, is I actually needed to test the boundaries of an application. So I needed to create like a multi terabyte data set so that I could check the performance and benchmark the application. The third thing, and which is quite useful if you try it, which is development acceleration, a lot of times you're dependent on upstream teams to give you your data set, but either they're running slow or they don't have time.

Or even if they give you the data set, they give you a very small data set. So you can't actually test on that scale. So the, if you create a synthetic data set, essentially you decouple yourself, which is you can develop your whole pipeline without actually seeing the data set. Whenever the upstream team provides you the data set, you can quickly check your pipeline against their data set.

The hardware I used was a four core machine with 32 gigs of RAM. I use Photon in my example. The first thing we're going to install is a library called DLB data gen. We are installing a library to generate the data sets, a couple of imports, the number of partitions. I have four cores, so I ask Parquet to generate data in four partitions. You can change this number. I'm generating a million rows a second. You can go higher based on what I found.

A million and four cores is a good combination. If you're trying to do two million, I would say get eight cores. After that, you will see that I'm specifying a schema. I'm generating IoT data. So I am specifying the schema. I'm using this particular library, which is helping me generate the data set. I'm just creating a streaming data frame. Streaming data frame is just a data frame. The streaming means it's being generated continuously.

One thing which people don't know is I see people miss very often is they don't give their streaming queries a name. Spark streaming has this parameter called query name. You can give your streaming queries a name. And once it shows up on Spark UI, you will see the name of the query along with the streaming ID of the query.

The beauty of the data generator is that it generates data close to your real data set. You'll have to specify what makes it real for your use case. So yeah, if you do this, you'll have your data and let's check where we reached you here again, see this numbers would keep bouncing, but in general, what you want to see is if you look at this graph input and processing rate remain close to each other.

This is going to be cheaper than your Starbucks coffee. On a four core setup, you're going to take approximately 17 minutes to generate a billion rows. Add extra five minutes as a buffer to actually get an instance. So 17 and five together is 22 minutes and 22 by 60 is 0.36 hours. Then I did the math and added the EC2 and Databricks cost together. It's coming out to be 22 cents, which is amazing.

---





---
*Skill auto-generated from YouTube transcript*
*Created: 2026-02-07*
