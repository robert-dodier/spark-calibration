### About spark-calibration
spark-calibration is a package to to assess binary classifier
calibration (i.e., how well classifier outputs match observed class
proportions) in Spark.

This same code has been submitted as a pull request
[PR #10666](https://github.com/apache/spark/pull/10666), so, if the PR
is accepted, this code will be incorporated into Spark proper,
but in the meantime, users can access the code via this little package.

### Building spark-calibration

spark-calibration uses SBT.

- `sbt compile` to compile the calibration code
- `sbt test` to compile and execute tests
- `sbt assembly` to produce a jar

### Running spark-calibration

Execute `sbt assembly` to produce a jar, which can be used by a stand-
alone program, or by `spark-shell` or other interactive environment.

An example using `spark-shell`:

```
$ spark-shell -classpath ./target/scala-2.10/spark-calibration_2.10.6-0.0.1-SNAPSHOT-ASSEMBLY.jar
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 1.5.1
      /_/

Using Scala version 2.10.4 (Java HotSpot(TM) Server VM, Java 1.8.0_60)
Type in expressions to have them evaluated.
Type :help for more information.
scala> import org.apache.spark.mllib.evaluation.MyBinaryClassificationMetrics
scala> val scoresAndLabels = for (i <- List.range(0, 1000)) yield
  { val x=Math.random(); val y=if (Math.random() < x) 1.0; else 0.0; (x, y) }
scoresAndLabels: List[(Double, Double)] = List((0.050924686970576394,0.0), (0.581307441124606,1.0),
(0.04337658367773878,0.0), (0.6947054568058959,1.0), (0.14476764053189028,0.0), (0.7187185487961024,1.0),
(0.4619757085619838,1.0), (0.6862690206055306,1.0), (0.6539286601176615,1.0), (0.8493379415074935,1.0),
(0.3386535416831381,0.0), (0.468150341585331,1.0), (0.7980831756392178,1.0), (0.8660893101186515,1.0),
(0.4783308932000677,1.0), (0.5473667321305656,1.0), (0.5458401477536274,0.0), (0.89029840530767,1.0),
(0.6787609014304523,1.0), (0.3011017865491017,1.0), (0.3480994790606605,0.0), (0.7414549833501596,1.0),
(0.07463486069415037,0.0), (0.22762565126587542,1.0), (0.5352887688929333,0.0), (0.23979924281293608,0.0),
(0.9365828422245419,1.0), (0.9773056233024029,1.0), (0.7730109448795002...
scala> val metrics = new MyBinaryClassificationMetrics (sc.parallelize (scoresAndLabels), 10)
scala> metrics.calibration ().collect ()
res0: Array[((Double, Double), (Double, Long))] = Array(((4.0472235617361463E-4,0.10858073374053812),(0.09,100)),
((0.10973926574760517,0.2115801225109496),(0.13,100)), ((0.21189372353884195,0.3022489979101467),(0.2,100)),
((0.3027471482014371,0.41604546437784096),(0.31,100)), ((0.41819810137276603,0.4957384342016371),(0.5066666666666667,75)),
((0.4959595797555152,0.5923538222485368),(0.56,100)), ((0.5951368583668936,0.6905766703046999),(0.66,100)),
((0.6921109209812906,0.782857859097095),(0.78,100)), ((0.7864486201993359,0.8832296286091946),(0.9,100)),
((0.8832597316746916,0.9759519649158341),(0.91,100)), ((0.9760586638274226,0.9983358483048783),(1.0,25)))
```
