package com.test.weibo;

import java.io.IOException;

public class Weibo {

    private static final String NAME_SPACE = "weibo";

    // 用户关系表
    private static final String RELATION_TABLE = NAME_SPACE + "relation";
    private static final String CONTENT_TABLE = NAME_SPACE + "content";
    private static final String INBOX_TABLE = NAME_SPACE + "inbox";

    public static void init() throws IOException {
        WeiBoUtil.createNamespace(NAME_SPACE);
        /**
         * 用户关系表
         * 微博内容表
         * 收件箱表
         */
        WeiBoUtil.createTable(RELATION_TABLE, 1, "attends", "fans");
        WeiBoUtil.createTable(CONTENT_TABLE, 1, "info");
        WeiBoUtil.createTable(INBOX_TABLE, 100, "info");
    }

    public static void main(String[] args) throws IOException {
//        init();
    }
}
