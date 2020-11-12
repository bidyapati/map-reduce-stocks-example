package com.bidya.bd.stocks;

public class Utils {
	
	/*
	Stocks csv columns
	0 exchange name
	1 stock id
	2 dt
	3 open
	4 high
	5 low
	6 close
	7 volume
	8 adj_close

	 */
    public final static Integer STOCKS_NAME = 0;
    public final static Integer STOCKS_ID = 1;
    public final static Integer STOCKS_DATE = 2;
    public final static Integer STOCKS_OPEN = 3;
    public final static Integer STOCKS_HIGH = 4;
    public final static Integer STOCKS_LOW = 5;
    public final static Integer STOCKS_CLOSE = 6;
    public final static Integer STOCKS_VOLUME = 7;
    public final static Integer STOCKS_ADJ_CLOSE = 8;
    
    
    
    /*
     * Tasks
     */
    public final static Integer TASK_SUM = 0;
    public final static Integer TASK_MAX = 1;
    public final static Integer TASK_MIN = 2;
    public final static Integer TASK_AVG = 3;
}
