package com.longtech.mqtt;

import com.longtech.mqtt.Utils.CommonUtils;
import io.netty.util.internal.StringUtil;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by kaiguo on 2018/12/14.
 */
public class MqttWildcardTopicManager {
    private static MqttWildcardTopicManager _instance = null;

    public static void init() {
        _instance = new MqttWildcardTopicManager();
    }

    public static MqttWildcardTopicManager getInstance() {
        return _instance;
    }

    public static boolean isWildcardTopic( String topic ) {

        if(StringUtil.isNullOrEmpty(topic)) {
            return false;
        }

        if( topic.startsWith("#/") || topic.startsWith("+/")) {
            return true;
        }

        if( topic.endsWith("/#") || topic.endsWith("/+")) {
            return true;
        }

        if( topic.indexOf("/#/") >=0 || topic.indexOf("/+/") >= 0 ) {
            return true;
        }

        return false;
    }


    ConcurrentHashMap<String, ConcurrentSkipListSet<MqttSession>> wildcardTopicSessions = new ConcurrentHashMap<>();

    public long getWildTopicSessionsSize() {
        if( useTreeMode ) {
            return NodeHelper.getAllTopicSize();
        }
        return wildcardTopicSessions.size();
    }

    public ConcurrentHashMap<String, ConcurrentSkipListSet<MqttSession>> getWildcardTopicSessions() {
        return wildcardTopicSessions;
    }

    final boolean useTreeMode = true;

    public void addTopic(String topic, MqttSession session) {

        if( useTreeMode ) {
            NodeHelper.subSucrible(topic, session);
            return;
        }

        ConcurrentSkipListSet<MqttSession> sessions = CommonUtils.getBean(wildcardTopicSessions, topic, ConcurrentSkipListSet.class);
        sessions.add(session);
    }

    public void removeTopic( String topic, MqttSession session ) {
        if( useTreeMode ) {
            NodeHelper.unSubSucrible(topic, session);
            return;
        }
        ConcurrentSkipListSet<MqttSession> sessions = CommonUtils.getBean(wildcardTopicSessions, topic, ConcurrentSkipListSet.class);
        sessions.remove(session);
        if( sessions.size() == 0 ) {
            wildcardTopicSessions.remove(topic);
        }
    }

    public static void  processMessage(String filter, String topic,  ConcurrentSkipListSet set, byte[] data) {

        if(  isMatchFast(filter, topic)) {
                ConcurrentSkipListSet<MqttSession> sessions = set;
                for( MqttSession session : sessions) {
                    session.sendData(topic, data);
//                    System.out.println(session.getSid());
                }
        }
    }

    public void PublishMessage(final String topic, final byte[] data) {

        if( useTreeMode ) {
            ArrayList<MqttSession> allSessions = new ArrayList<>();
            NodeHelper.subscriptionSearch(topic, allSessions);
            for( MqttSession session : allSessions) {
                session.sendData(topic, data);
//                    System.out.println(session.getSid());
            }
            return;
        }


        ConcurrentSkipListSet set = null;
        ArrayList<MqttSession> allSessions = new ArrayList<>();
        for( Map.Entry<String, ConcurrentSkipListSet<MqttSession>> item : wildcardTopicSessions.entrySet()) {
            if( isMatchFast(item.getKey(), topic)) {
                ConcurrentSkipListSet<MqttSession> sessions = item.getValue();
                for( MqttSession session : sessions) {
                    session.sendData(topic, data);
//                    System.out.println(session.getSid());
                }
            }
        }
    }

    public static boolean isMatch(String filter , String topic) {


        String[] topicItem = topic.split("\\/");
        String[] filterItem = filter.split("\\/");
        int len = filterItem.length;
        for (int i = 0; i < len; ++i) {
            String left = filterItem[i];
            if (left.equals("#")) return true;
            String right = null;
            if( i < topicItem.length ) {
                right = topicItem[i];
            }
            else {
                return false;
            }
            if (!left.equals("+") && !left.equals(right)) return false;
        }
        return  len == topicItem.length;
    }


    public static boolean isMatchFast(String filter , String topicStr) {
        int slen, tlen;
        int spos, tpos;
        boolean multilevel_wildcard = false;
        boolean result = false;

        String sub = filter;
        String topic = topicStr;
        slen = sub.length();
        tlen = topic.length();

        if(slen > 0 && tlen > 0){
            if((sub.charAt(0) == '$' && topic.charAt(0) != '$')
                    || (topic.charAt(0) == '$' && sub.charAt(0) != '$')){

//                *result = false;
                return false;
//                return MOSQ_ERR_SUCCESS;
            }
        }

        spos = 0;
        tpos = 0;

        while(spos < slen && tpos < tlen){
            if(sub.charAt(spos) == topic.charAt(tpos)){
                if(tpos == tlen-1){
                /* Check for e.g. foo matching foo/# */
                    if(spos == slen-3
                            && sub.charAt(spos+1) == '/'
                            && sub.charAt(spos+2) == '#'){
                        result = true;
                        multilevel_wildcard = true;
                        return result;
                    }
                }
                spos++;
                tpos++;
                if(spos == slen && tpos == tlen){
                    result = true;
                    return result;
                }else if(tpos == tlen && spos == slen-1 && sub.charAt(spos) == '+'){
                    spos++;
                    result = true;
                    return result;
                }
            }else{
                if(sub.charAt(spos) == '+'){
                    spos++;
                    while(tpos < tlen && topic.charAt(tpos) != '/'){
                        tpos++;
                    }
                    if(tpos == tlen && spos == slen){
                        result = true;
                        return result;
                    }
                }else if(sub.charAt(spos) == '#'){
                    multilevel_wildcard = true;
                    if(spos+1 != slen){
                        result = false;
                        return result;
                    }else{
                        result = true;
                        return result;
                    }
                }else{
                    result = false;
                    return result;
                }
            }
        }
        if(multilevel_wildcard == false && (tpos < tlen || spos < slen)){
            result = false;
        }

        return result;
    }

    public static Node Root = new Node();
    public static ArrayList<String> result = new ArrayList<>();
    public static class Node {
        ConcurrentSkipListSet<MqttSession> subs = new ConcurrentSkipListSet<>();
        public ConcurrentHashMap<String, Node> children = new ConcurrentHashMap<>();
        //        AtomicInteger kidsCount = new AtomicInteger();
//        AtomicInteger subsCount = new AtomicInteger();
        Node parent = null;
    }

    public static class NodeHelper {


        public static long getAllTopicSize() {
            return getNodeSize(Root);
        }

        public static long getNodeSize( Node root ) {

            if( root == null ) {
                return  0;
            }

            long ret = 0;

            if( root.subs.size() > 0 ) {
                ret = 1;
            }

            for( Map.Entry<String, Node> child : root.children.entrySet() ) {
                ret += getNodeSize(child.getValue());
            }

            return ret;
        }

        public static Node newNode( Node parent ) {
            Node node = new Node();
            node.parent = parent;
            return node;
        }

        public static Node leafInsertNode( String[] levels ) {
            Node root = Root;
            for( int i = 0; i < levels.length; i++ ) {
                Node newNode =  newNode(root);
                Node n = root.children.putIfAbsent(levels[i], newNode);
                if( n == null ) {
                    n = newNode;
                }

//                    n.subsCount += 1;
                root = n;
            }
            return root;
        }

        public static Node leafSearchNode( String[] levels)  {
            Node root = Root;

            for( int i = 0; i < levels.length; i++ ) {
                Node n = root.children.get(levels[i]);
                if( n == null ) {
                    return  null;
                }
                root = n;
            }
            return root;
        }



        public static void subSucrible( String topic, MqttSession session) {
            String[] topicItem = topic.split("\\/");
            Node node = leafInsertNode(topicItem);
            node.subs.add(session);
        }

        public static void unSubSucrible( String topic, MqttSession session) {
            String[] topicItem = topic.split("\\/");
            Node node = leafSearchNode(topicItem);
            if( node != null ) {
                node.subs.remove(session);
                nodeCleanup(node, topicItem);

            }

        }

        public static void nodeCleanup(Node root, String[] levels) {
            int level = levels.length;
            for( Node leafNode = root; leafNode != null; leafNode = leafNode.parent) {
                if( leafNode.subs.size() == 0 && leafNode.children.size() == 0 ) {
                    if( leafNode.parent != null ) {
                        leafNode.parent.children.remove(levels[level - 1]);
                    }
                }
                level--;
            }
        }

        public static void subscriptionSearch(String topic, ArrayList<MqttSession> session ) {
            String[] levels = topic.split("\\/");
            subscriptionRecurseSearch(Root, levels, 0, session);
//            for( String item : result) {
//                System.out.println("findTopic: " + item);
//            }
        }



        public static void subscriptionRecurseSearch(Node root,  String[] levels, int offset, ArrayList<MqttSession> session) {
            if( levels.length == offset ) {
                session.addAll(root.subs);
                if( root.children.containsKey("#") ) {
                    session.addAll(root.children.get("#").subs);
                }
            }
            else {
                if( root.children.containsKey("#") && levels[offset].length() > 0 ) {
                    session.addAll(root.children.get("#").subs);
                }

                if( root.children.containsKey(levels[offset])) {
                    subscriptionRecurseSearch(root.children.get(levels[offset]), levels, offset + 1,session);
                }

                if( root.children.containsKey("+")) {
                    subscriptionRecurseSearch(root.children.get("+"),levels, offset + 1, session);
                }
            }
        }
    }

//    public static boolean isMatchFast2(String filter , String topicStr) {
//
//    }


    public static void main(String[] args) {

//        long curTime = System.currentTimeMillis() * 1000;
//        long nanoTime = System.nanoTime();
//        long macroTime = curTime + ( nanoTime - nanoTime/1000000 *1000000) / 1000;
//
//        System.out.printf(curTime + " " + nanoTime + " " + macroTime);
//        if( true )return;

        MqttWildcardTopicManager.init();

        for( int i = 0; i < 1000000; i++ ) {
            String topic = "/hello/123456/+/" + i + "/42423432/+/42443/#";
            MqttWildcardTopicManager.getInstance().addTopic(topic, new MqttSession());
//            Node.subSucrible(topic, i+"");
        }



        int i = 2500;
        long start = System.currentTimeMillis();
//        Node.subscriptionSearch("/hello/123456/afe/" + i + "/42423432/abc/42443/test");
        MqttWildcardTopicManager.getInstance().PublishMessage( "/hello/123456/afe/" + i + "/42423432/abc/42443/test", "hello".getBytes());
        long end = System.currentTimeMillis();
        System.out.printf("Time spend: " + (end - start));
    }
}
