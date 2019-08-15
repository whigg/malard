sbt -Dconfig.resource=queryprocessor1.conf "point-stream-impl/runMain com.earthwave.pointstream.impl.PointWorkerProcess -instance=1 -worker=QueryProcessor" &
P1=$!
sbt -Dconfig.resource=queryprocessor2.conf "point-stream-impl/runMain com.earthwave.pointstream.impl.PointWorkerProcess -instance=2 -worker=QueryProcessor" &
P2=$!
sbt -Dconfig.resource=queryprocessor3.conf "point-stream-impl/runMain com.earthwave.pointstream.impl.PointWorkerProcess -instance=3 -worker=QueryProcessor" &
P3=$!
sbt -Dconfig.resource=queryprocessor4.conf "point-stream-impl/runMain com.earthwave.pointstream.impl.PointWorkerProcess -instance=4 -worker=QueryProcessor" &
P4=$!
sbt -Dconfig.resource=swathProcessor1.conf "point-stream-impl/runMain com.earthwave.pointstream.impl.PointWorkerProcess -instance=1 -worker=SwathProcessor" &
P5=$!
sbt -Dconfig.resource=swathProcessor2.conf "point-stream-impl/runMain com.earthwave.pointstream.impl.PointWorkerProcess -instance=2 -worker=SwathProcessor" &
P6=$!
sbt -Dconfig.resource=swathProcessor3.conf "point-stream-impl/runMain com.earthwave.pointstream.impl.PointWorkerProcess -instance=3 -worker=SwathProcessor" &
P7=$!
sbt -Dconfig.resource=swathProcessor4.conf "point-stream-impl/runMain com.earthwave.pointstream.impl.PointWorkerProcess -instance=4 -worker=SwathProcessor" &
P8=$!
sbt -Dconfig.resource=publishprocessor1.conf "point-stream-impl/runMain com.earthwave.pointstream.impl.PointWorkerProcess -instance=1 -worker=PublisherProcessor" &
P9=$!
sbt -Dconfig.resource=publishprocessor2.conf "point-stream-impl/runMain com.earthwave.pointstream.impl.PointWorkerProcess -instance=2 -worker=PublisherProcessor" &
P10=$!
sbt -Dconfig.resource=publishprocessor3.conf "point-stream-impl/runMain com.earthwave.pointstream.impl.PointWorkerProcess -instance=3 -worker=PublisherProcessor" &
P11=$!
sbt -Dconfig.resource=publishprocessor4.conf "point-stream-impl/runMain com.earthwave.pointstream.impl.PointWorkerProcess -instance=4 -worker=PublisherProcessor" &
P12=$!

wait $P1 $P2 $P3 $P4 $P5 $P6 $P7 $P8 $P9 $P10 $P11 $P12


