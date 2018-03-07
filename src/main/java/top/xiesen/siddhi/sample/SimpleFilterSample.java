package top.xiesen.siddhi.sample;

import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;
import top.xiesen.siddhi.utils.DateUtil;


/**
 * Author: xiesen
 * Description: siddhi demo，数据过滤
 * Date: Created in 13:06 2018/3/7
 */
public class SimpleFilterSample {
    public static void main(String[] args) throws Exception {
        // Creating Siddhi Application
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "define stream StockEventStream (symbol string, price float, volume long); " +
                " " +
                "@info(name = 'query1') " +
                "from StockEventStream#window.time(5 sec)  " +
                "select symbol, sum(price) as price, sum(volume) as volume " +
                "group by symbol " +
                "insert into AggregateStockStream ;";
        // Creating Siddhi Application Runtime
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        // Registering a Callback
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
//                EventPrinter.print(timeStamp, inEvents, removeEvents)
                System.out.print(inEvents[0].getData(0) + " ");
            }
        });

        //  Sending Events
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StockEventStream");
        // Start SiddhiApp runtime
        siddhiAppRuntime.start();

        // Sending events to Siddhi
        inputHandler.send(new Object[]{"Welcome",700f,1000L});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 200L});
        inputHandler.send(new Object[]{"to", 50f, 30L});
        inputHandler.send(new Object[]{"IBM", 76.6f, 400L});
        inputHandler.send(new Object[]{"siddhi!", 45.6f, 50L});
        Thread.sleep(500);

        // Shutting down the runtime
        siddhiAppRuntime.shutdown();

        // Shutting down Siddhi
        siddhiManager.shutdown();
    }
}
