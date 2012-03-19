package scamr.conf

import org.apache.hadoop.conf.Configuration

trait ConfModifier extends ConfOrJobModifier with Function1[Configuration, Unit];