import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;

public class Example {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        final DataStream<Order> orderStream = env.fromElements(
                Order.of(1000L, "Order_1"),
                Order.of(2000L, "Order_1"),
                Order.of(3000L, "Order_1"),
                Order.of(4000L, "Order_1"),
                Order.of(5000L, "Order_1"),
                Order.of(6000L, "Order_1"));

        Table orders = tableEnv.fromDataStream(orderStream, Schema.newBuilder()
            .column("order_time", DataTypes.BIGINT())
            .column("orderID", DataTypes.STRING())
            .build());

        tableEnv.createTemporaryView("orders", orders);

        final Table result = tableEnv.sqlQuery(
            "SELECT orderID, count(order_time) AS c FROM orders GROUP BY orderID HAVING count(order_time)=1;");

        result.execute().print();
        /*
        +----+--------------------------------+----------------------+
        | op |                        orderID |                    c |
        +----+--------------------------------+----------------------+
        | +I |                        Order_1 |                    1 |
        | -U |                        Order_1 |                    1 |
        +----+--------------------------------+----------------------+
         */

        DataStream<Row> x = tableEnv.toChangelogStream( // default to upsert, i.e. no -U items
            result,
            Schema.newBuilder()
                .fromResolvedSchema(result.getResolvedSchema())
                .build(),
            ChangelogMode.upsert());
        x.print();
        /*
        6> +I[Order_1, 1]
        6> -U[Order_1, 1]
         */

        env.execute();
    }

    public static class Order {
        public long order_time;
        public String orderID;

        public static Order of(long order_time, String orderID) {
            Order result = new Order();
            result.order_time = order_time;
            result.orderID = orderID;
            return result;
        }
    }
}
