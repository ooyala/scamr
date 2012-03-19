package scamr.conf

import org.apache.hadoop.mapreduce.Job

trait JobModifier extends ConfOrJobModifier with Function1[Job, Unit];
