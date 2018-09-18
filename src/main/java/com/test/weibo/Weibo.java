package com.test.weibo;

import java.io.IOException;

public class Weibo {

    public static void init() throws IOException {
        WeiBoUtil.createNamespace(Contents.NAME_SPACE);
        /**
         * 用户关系表
         * 微博内容表
         * 收件箱表
         */
        WeiBoUtil.createTable(Contents.RELATION_TABLE, 1, "attends", "fans");
        WeiBoUtil.createTable(Contents.CONTENT_TABLE, 1, "info");
        WeiBoUtil.createTable(Contents.INBOX_TABLE, 100, "info");
    }

    public static void main(String[] args) throws IOException {
//        init();

        // 关注
        WeiBoUtil.addAttends("1001", "1002", "1003");

        // 被关注的人发微博(多个人发布微博)
        WeiBoUtil.putData(Contents.CONTENT_TABLE, "1001", "info", "content", "九月十九号 1001");
        WeiBoUtil.putData(Contents.CONTENT_TABLE, "1002", "info", "content", "Hello World 1002");
        WeiBoUtil.putData(Contents.CONTENT_TABLE, "1002", "info", "content", "测试内容 1002");
        WeiBoUtil.putData(Contents.CONTENT_TABLE, "1003", "info", "content", "五点十四分 1003");

        // 获取关注人的微博
        WeiBoUtil.getWeibo("1001");

        // 关注已经发过微博的人

        // 获取关注人的微博

        // 取消关注

        // 取消关注人的微博
    }
}
