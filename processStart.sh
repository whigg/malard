sbt -Dconfig.resource=queryprocessor1.conf "point-stream-impl/runMain com.earthwave.pointstream.impl.PointWorkerProcess -instance=1 -worker=QueryProcessor" &
P1=$!
sbt -Dconfig.resource=swathProcessor1.conf "point-stream-impl/runMain com.earthwave.pointstream.impl.PointWorkerProcess -instance=1 -worker=SwathProcessor" &
P5=$!
sbt -Dconfig.resource=publishprocessor1.conf "point-stream-impl/runMain com.earthwave.pointstream.impl.PointWorkerProcess -instance=1 -worker=PublisherProcessor" &
P9=$!
sbt -Dconfig.resource=publishprocessor2.conf "point-stream-impl/runMain com.earthwave.pointstream.impl.PointWorkerProcess -instance=2 -worker=PublisherProcessor" &
P10=$!
sbt -Dconfig.resource=publishprocessor3.conf "point-stream-impl/runMain com.earthwave.pointstream.impl.PointWorkerProcess -instance=3 -worker=PublisherProcessor" &
P11=$!

wait $P1 $P5 $P9 $P10 $P11


