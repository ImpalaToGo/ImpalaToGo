Extra Hadoop classes from CDH5 needed by Impala to support CDH4.

This is necessary because Impala has an admission controller that is configured
using the same configuration as Llama and Yarn (i.e. a fair-scheduler.xml and
llama-site.xml). Some Yarn classes (available in CDH5 only) are used to provide
user to pool resolution, authorization, and accessing pool configurations.
Impala needs these classes on the classpath in order to support CDH4, so they're
built as a separate third-party jar. For CDH5, this jar is not necessary and
will not be included as a dependency. Because this exists in CDH4 only, this
lives in third-party along with other CDH version-specific dependencies.
