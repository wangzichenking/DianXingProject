package com.wzc;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class ProducerLog {

    Map contacts = null;
    List<String> phoneList = null;


    public void initContacts() {
        contacts = new HashMap<String, String>();
        phoneList = new ArrayList<String>();

        phoneList.add("15369468720");
        phoneList.add("19920860202");
        phoneList.add("18411925860");
        phoneList.add("14473548449");
        phoneList.add("18749966182");
        phoneList.add("19379884788");
        phoneList.add("19335715448");
        phoneList.add("18503558939");
        phoneList.add("13407209608");
        phoneList.add("15596505995");
        phoneList.add("17519874292");
        phoneList.add("15178485516");
        phoneList.add("19877232369");
        phoneList.add("18706287692");
        phoneList.add("18944239644");
        phoneList.add("17325302007");
        phoneList.add("18839074540");
        phoneList.add("19879419704");
        phoneList.add("16480981069");
        phoneList.add("18674257265");
        phoneList.add("18302820904");
        phoneList.add("15133295266");
        phoneList.add("17868457605");
        phoneList.add("15490732767");
        phoneList.add("15064972307");

        contacts.put("15369468720", "李雁");
        contacts.put("19920860202", "卫艺");
        contacts.put("18411925860", "仰莉");
        contacts.put("14473548449", "陶欣悦");
        contacts.put("18749966182", "施梅梅");
        contacts.put("19379884788", "金虹霖");
        contacts.put("19335715448", "魏明艳");
        contacts.put("18503558939", "华贞");
        contacts.put("13407209608", "华啟倩");
        contacts.put("15596505995", "仲采绿");
        contacts.put("17519874292", "卫丹");
        contacts.put("15178485516", "戚丽红");
        contacts.put("19877232369", "何翠柔");
        contacts.put("18706287692", "钱溶艳");
        contacts.put("18944239644", "钱琳");
        contacts.put("17325302007", "缪静欣");
        contacts.put("18839074540", "焦秋菊");
        contacts.put("19879419704", "吕访琴");
        contacts.put("16480981069", "沈丹");
        contacts.put("18674257265", "褚美丽");
        contacts.put("18302820904", "孙怡");
        contacts.put("15133295266", "许婵");
        contacts.put("17868457605", "曹红恋");
        contacts.put("15490732767", "吕柔");
        contacts.put("15064972307", "冯怜云");
    }


    //随机生成两个电话号


    //随机生成通话建立时间
    private String randomDate(String startDate, String endDate) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        try {
            Date start = simpleDateFormat.parse(startDate);
            Date end = simpleDateFormat.parse(endDate);

            if (start.getTime() > end.getTime()) return null;

            long resultTime = start.getTime() + (long) (Math.random() * (end.getTime() - start.getTime()));
            /*
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(resultTime);
             */

            return resultTime+"";

        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    //拼接日志
    private String productLog() {
        //随机数的范围是0-phoneList.size()
        int call1Index = new Random().nextInt(phoneList.size());
        int call2Index = -1;

        String call1 = phoneList.get(call1Index);
        String call2 = null;
        while (true) {
            call2Index = new Random().nextInt(phoneList.size());
            call2 = phoneList.get(call2Index);
            if (!call1.equals(call2)) break;
        }

        //随机生成通话时长，单位：秒
        int duration = new Random().nextInt(60 * 30) + 1;
        //格式化通话时间
        String durationString = new DecimalFormat("0000").format(duration);

        //通话建立时间:yyyy-MM-dd,月份：0~11，天：1~31
        String randomDate = randomDate("2019-06-01", "2019-12-30");
        String dateString = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Long.parseLong(randomDate));

        //拼接log日志
        StringBuilder logBuilder = new StringBuilder();

//        logBuilder.append(caller + ",")
//                .append(callerName + ",")
//                .append(callee + ",")
//                .append(calleeName + ",")
//                .append(dateString + ",")
//                .append(durationString);
        logBuilder.append(call1 + ",")
                .append(call2 + ",")
                .append(dateString + ",")
                .append(durationString);
        System.out.println(logBuilder);

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return logBuilder.toString();
    }

    //写日志方法
    public void writeLog(String filePath, ProducerLog producerLog) {
        //输出流
        OutputStreamWriter outputStreamWriter = null;
        try {
            outputStreamWriter = new OutputStreamWriter(new FileOutputStream(filePath, true),"UTF-8");
//            for (int i = 1; i <= CALL_COUNT; i++) {
//                String log = productLog.productLog();
//                if(i == CALL_COUNT){
//                    outputStreamWriter.write(log);
//                }else{
//                    outputStreamWriter.write(log + "\n");
//                }
//            }
            while(true){
                String log = producerLog.productLog();
                outputStreamWriter.write(log + "\n");
                outputStreamWriter.flush();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                outputStreamWriter.flush();
                outputStreamWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        if(args == null || args.length <= 0) {
            System.out.println("no arguments");
            System.exit(1);
//            args = new String[]{PATH_LOG};
        }
        ProducerLog productLog = new ProducerLog();
        productLog.initContacts();
        productLog.writeLog(args[0], productLog);
    }

}
