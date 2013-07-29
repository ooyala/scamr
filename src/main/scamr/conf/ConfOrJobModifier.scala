package scamr.conf

import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.conf.Configuration

// Base trait for all modifiers
trait ConfOrJobModifier
// Modifiers that apply to a hadoop Configuration object
trait ConfModifier extends ConfOrJobModifier with Function1[Configuration, Unit]
// Modifiers that apply to a hadoop Job object
trait JobModifier extends ConfOrJobModifier with Function1[Job, Unit]
