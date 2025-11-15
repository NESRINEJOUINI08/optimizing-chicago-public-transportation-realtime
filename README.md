# Optimizing Public Transportation with Apache Kafka

## Overview

This project builds a real-time streaming system for the Chicago Transit Authority (CTA) using Apache Kafka and its ecosystem. The goal is to simulate and monitor the status of CTA train lines so that commuters can see live train movements, station activity, and ridership trends on a web dashboard.

The system ingests events such as train arrivals, turnstile entries, and weather conditions, processes them through stream processing and aggregation tools, and exposes a simple web UI to visualize the current state of the network.

### Data Sources & Event Types

The system models several key data sources:

#### Station Arrivals
Each time a train arrives at a station, an event is emitted with:

Station ID

Train ID

Direction

Line color

Train status

Previous station and direction

#### Turnstile Events
Turnstiles at each station emit an event whenever a rider enters the system, including:

Station ID

Station name

Line color

#### Weather Events
A simulated weather sensor periodically sends:

Temperature

Current weather status (sunny, cloudy, etc.)

#### Station Metadata (PostgreSQL → Kafka)
The CTA station reference data (stop_id, station name, line colors, etc.) lives in PostgreSQL and is imported into Kafka using Kafka Connect.

All of these events are encoded using Avro schemas and registered in Schema Registry.

### System Architecture

The project demonstrates a full streaming pipeline that includes:

Producers

Kafka Connect (JDBC Source)

Stream Processing with Faust

Aggregation with KSQL

Consumers and Web Application

#### At a high level:

Producers publish events into Kafka topics.

Kafka Connect continuously ingests static metadata from PostgreSQL.

Faust transforms and normalizes station metadata into a convenient format.

KSQL aggregates turnstile events into station-level counts.

A Python consumer layer maintains in-memory models for lines, stations, and weather and powers the transit status web page.

### Components
1. Producers

Custom Python producers generate:

Arrival events on per-station topics (for example, one topic per station for arrivals).

Turnstile events for each station turnstile.

Weather events via Kafka REST Proxy, simulating the behavior of legacy hardware that can only communicate over HTTP.

Each producer:

Uses Avro key and value schemas.

Creates topics if they do not already exist.

Encapsulates common logic in a reusable Producer base class.

2. Kafka Connect (JDBC Source Connector)

Kafka Connect is used to load station metadata from a PostgreSQL database into Kafka.

The JDBC Source Connector connects to the cta database.

It reads from the stations table.

It uses incrementing mode on the stop_id column.

Records are written into a Kafka topic with a CTA-specific prefix.

This allows the rest of the system to treat station metadata as a stream of records rather than static data.

3. Stream Processing with Faust

Faust is used to clean and enrich station metadata:

It consumes station records produced by Kafka Connect.

It maps each record into a strongly typed Station model.

It determines the line color (red, green, or blue) based on boolean flags and normalizes the data.

It maintains a Faust table representing the latest state of all stations.

It publishes a transformed station topic that is easier for downstream consumers to work with.

This step isolates schema quirks and ensures the consumer and web UI see a consistent station representation.

4. Aggregation with KSQL

KSQL runs continuous queries on the turnstile event stream to maintain station-level rider counts:

A KSQL stream is defined over the raw turnstile events topic.

A KSQL table aggregates events by station, counting total entries per station.

The result is materialized as a table topic (for example, TURNSTILE_SUMMARY).

This provides an always-up-to-date view of ridership without needing to manually maintain counters in application code.

5. Consumers and Web Application

On the consumer side, the project includes:

A generic Kafka consumer that:

Subscribes to weather, station table, arrival, and turnstile summary topics.

Can consume Avro and JSON payloads.

Uses an asynchronous polling loop to keep models in sync with Kafka.

Domain models:

Weather model
Stores current temperature and status.

Station model
Stores station metadata, current trains in each direction, and accumulated turnstile entries.

Line model
Manages a collection of stations for a given line color (red, green, blue) and updates them based on messages.

Lines wrapper
Routes incoming messages to the appropriate line or to all lines, depending on topic.

A web server backed by Tornado:

Uses the in-memory line, station, and weather models.

Serves a transit status page that shows:

Stations grouped by line,

Current train positions and statuses (by direction),

Turnstile entry counts,

Current weather conditions.

### End-to-End Data Flow

Station information is loaded from PostgreSQL into Kafka via Kafka Connect.

Faust consumes the station topic, transforms records, and writes back a normalized station table topic.

The simulation produces train arrival and turnstile events into Kafka topics.

KSQL reads turnstile events, maintains a station-level count, and publishes an aggregated summary.

A consumer service subscribes to:

The Faust-transformed station table,

The arrival events,

The KSQL turnstile summary,

The weather topic.

The consumer updates in-memory models used by the web server.

The web UI queries these models to render a real-time view of the CTA network.

### What This Project Demonstrates

Designing an event-driven architecture for a real-time system.

Modeling domain events (arrivals, turnstiles, weather) using Avro.

Using Kafka Connect to bridge a relational database into Kafka.

Applying stream processing (Faust) for transformation and normalization.

Using KSQL for continuous aggregation and materialized views.

Integrating Kafka consumers with a web application to power a live dashboard.