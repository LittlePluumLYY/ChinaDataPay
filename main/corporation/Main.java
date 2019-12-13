import com.alibaba.fastjson.JSON;
import lombok.Data;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.*;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

public class Main {

    static Properties prop = new Properties();

    static String host;
    static String servicePath;
    static String queryFrom;

    public static void main(String[] args) {
        String userDir = System.getProperty("user.dir");
        String resRelativePath = "resource/config.properties";
        try {
            prop.load(new BufferedInputStream(new FileInputStream(userDir+"/"+resRelativePath)));
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        host = prop.getProperty("QueryHost");
        servicePath = prop.getProperty("Corporation.SerivcePath");
        queryFrom = prop.getProperty("Corporation.QueryFrom");

        String sourceFilePath = prop.getProperty("Corporation.SourceFile");

        File file = new File(userDir+sourceFilePath);
        if (!file.exists()) {
            System.out.println("source file not exsit!");
            return;
        }
        BufferedReader bufferedReader;
        try {
            bufferedReader = new BufferedReader(new FileReader((file)));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return;
        }

        String line;
        List<String> corps = new ArrayList<String>(1024);

        try {
            while ((line=bufferedReader.readLine()) != null) {
                String corp = line.trim();
                if (!"".equals(corp)) {
                    corps.add(corp);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            return;
        } finally {
            try {
                bufferedReader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        //单线程查询条数
        int limit = 200;
        int size = (corps.size()+limit-1)/limit;

        Worker[] workers = new Worker[size];
        CountDownLatch latch = new CountDownLatch(size);
        for (int i = 0; i < size; i++) {
            int start = i*limit;
            workers[i] = new Worker(latch, corps.subList(start, Math.min(corps.size(), start+limit)));
            workers[i].start();
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //write file
        File newFile = new File(file.getPath().replace(file.getName(), "infos-"+file.getName()));
        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new FileWriter(newFile));
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        try {
            writer.write("企业名称\t"+getColumns()+"\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (int i = 0; i < workers.length; i++) {
            List<DetailResult> results = workers[i].getResults();
            for (int j = 0; j < results.size(); j++) {
                try {
                    DetailResult detailResult = results.get(j);
                    writer.write(detailResult.getCorp()+"\t"+getContents(detailResult)+"\n");
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
        }

        try {
            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Data
    private static class Worker extends Thread {
        Worker(CountDownLatch latch, List<String> corps) {
            this.latch = latch;
            this.corps = corps;
        }
        CountDownLatch latch;
        List<String> corps;
        List<DetailResult> results;

        @Override
        public void run() {
            results = new ArrayList<>(corps.size());
            for (String cop : corps) {
                String result = null;
                try {
                    CloseableHttpClient client = HttpClients.createDefault();
                    HttpGet httpGet = new HttpGet(host+servicePath+ URLEncoder.encode(cop, "utf8")+"?"+queryFrom);
                    CloseableHttpResponse response = null;
                    try {
                        response = client.execute(httpGet);
                    } catch (IOException e) {
                        e.printStackTrace();
                        return;
                    }
                    result = EntityUtils.toString(response.getEntity(), "utf8");
                    client.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    return;
                }

                System.out.println(result);

                DetailResult detailResult = JSON.parseObject(result, DetailResult.class);
                detailResult.setCorp(cop);
                results.add(detailResult);

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            latch.countDown();
        }
    }

    /**
     * 获取列名
     *
     * @return
     */
    private static String getColumns() {
        Class<DetailResult> clazz = DetailResult.class;
        Field[] pFields = clazz.getDeclaredFields();

        List<String> listFieldsColumns = new ArrayList<>();
        List<String> fieldsColumns = new ArrayList<>();

        getColumns(pFields, listFieldsColumns, fieldsColumns);

        return fieldsColumns.stream().collect(Collectors.joining("\t")) + "\t" + listFieldsColumns.stream().collect(Collectors.joining("\t"));
    }

    private static void getColumns(Field[] pFields, List<String> listFieldsColumns, List<String> fieldsColumns) {
        for (int i = 0; i < pFields.length; i++) {
            Field pField = pFields[i];
            FieldDesc pFieldDesc = pField.getAnnotation(FieldDesc.class);
            if (pFieldDesc == null) {
                continue;
            }

            Class<?> cClazz = pField.getType();

            if (cClazz.isAssignableFrom(List.class)) {
//                listFieldsColumns.add(pFieldDesc.value());
                ParameterizedType stringListType = (ParameterizedType) pField.getGenericType();
                cClazz = (Class<?>) stringListType.getActualTypeArguments()[0];
            }

            if (Item.class.isAssignableFrom(cClazz)) {
                Field[] cFields = cClazz.getDeclaredFields();
                getColumns(cFields, listFieldsColumns, fieldsColumns);
            } else {
                fieldsColumns.add(pFieldDesc.value());
            }
        }
    }


    /**
     * 解析数据每项属性，tab分隔
     *
     * @param detailResult
     * @return
     * @throws IllegalAccessException
     */
    private static String getContents(DetailResult detailResult) throws IllegalAccessException {
        Class<DetailResult> clazz = DetailResult.class;
        Field[] pFields = clazz.getDeclaredFields();

        List<String> listFieldsContent = new ArrayList<>();
        List<String> fieldsContent = new ArrayList<>();

        getContents(pFields, listFieldsContent, fieldsContent, detailResult);

        return fieldsContent.stream().collect(Collectors.joining("\t")) + "\t" + listFieldsContent.stream().collect(Collectors.joining("\t"));
    }

    private static void getContents(Field[] pFields, List<String> listFieldsContent, List<String> fieldsContent, Object value) {
        for (int i = 0; i < pFields.length; i++) {
            Field pField = pFields[i];
            FieldDesc pFieldDesc = pField.getAnnotation(FieldDesc.class);
            if (pFieldDesc == null) {
                continue;
            }

            Class<?> cClazz = pField.getType();

            Object fieldValue = null;
            try {
                fieldValue = value == null ? null : pField.get(value);
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }

            if (cClazz.isAssignableFrom(List.class)) {
                ParameterizedType stringListType = (ParameterizedType) pField.getGenericType();
                cClazz = (Class<?>) stringListType.getActualTypeArguments()[0];

                if (fieldValue != null) {
                    List list = (List) fieldValue;
                    if (!list.isEmpty()) {
                        fieldValue = list.get(0);
                    } else {
                        fieldValue = null;
                    }
                }
            }

            if (Item.class.isAssignableFrom(cClazz)) {
                Field[] cFields = cClazz.getDeclaredFields();
                getContents(cFields, listFieldsContent, fieldsContent, fieldValue);
            } else {
                fieldsContent.add(fieldValue == null? "" : fieldValue.toString().replace("\n", ""));
            }
        }
    }

    @Data
    private static class DetailResult {
        //公司名称
        String corp;
        @FieldDesc(value = "基础信息")
        DetailBaseInfoItem baseInfo;
        @FieldDesc(value = "年报信息列表")
        List<DetailAnnualReportItem> annuRepYearList;
        @FieldDesc(value = "分支机构列表")
        List<DetailBranchOrgItem> branchList;
        @FieldDesc(value = "经营异常信息列表")
        List<DetailAbnormalItem> comAbnoInfoList;
        @FieldDesc(value = "变更信息列表")
        List<DetailChangeItem> comChanInfoList;
        @FieldDesc(value = "投资公司列表")
        List<DetailInvestCorpItem> investList;
        @FieldDesc(value = "股东列表")
        List<DetailInvestorItem> investorList;
        @FieldDesc(value = "诉讼信息列表")
        List<DetailLawsuitItem> lawSuitList;
        @FieldDesc(value = "高管列表")
        List<DetailSeniorStaffItem> staffList;
        @FieldDesc(value = "商标列表")
        List<DetailBrandItem> tmList;
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.FIELD})
    @interface FieldDesc {
        String value() default "";
    }

    interface Item {}

    /**
     * 基础信息
     */
    @Data
    private static class DetailBaseInfoItem implements Item {
        @FieldDesc(value = "统计项")
        List<DetailAllCountItem> allCount;
        @FieldDesc(value = "暂无")
        String actualCapital;
        @FieldDesc(value = "核准时间")
        Long approvedTime;
        @FieldDesc(value = "地址")
        String base;
        @FieldDesc(value = "经营范围")
        String businessScope;
        @FieldDesc(value = "行业评分")
        Long categoryScore;
        @FieldDesc(value = "公司类型")
        String companyOrgType;
        @FieldDesc(value = "暂无")
        Long companyType;
        @FieldDesc(value = "统一信用代码")
        String creditCode;
        @FieldDesc(value = "注册时间")
        Long estiblishTime;
        @FieldDesc(value = "暂无")
        Long flag;
        @FieldDesc(value = "营业期限开始时间")
        String fromTime;
        @FieldDesc(value = "公司id（查询用）")
        Long id;
        @FieldDesc(value = "行业")
        String industry;
        @FieldDesc(value = "法人id")
        Long legalPersonId;
        @FieldDesc(value = "法人名称")
        String legalPersonName;
        @FieldDesc(value = "公司名称")
        String name;
        @FieldDesc(value = "组织结构代码证颁发机关")
        String orgApprovedInstitute;
        @FieldDesc(value = "组织结构代码")
        String orgNumber;
        @FieldDesc(value = "公司评分")
        Long percentileScore;
        @FieldDesc(value = "联系电话")
        String phoneNumber;
        @FieldDesc(value = "注册资金")
        String regCapital;
        @FieldDesc(value = "登记机关")
        String regInstitute;
        @FieldDesc(value = "注册地址")
        String regLocation;
        @FieldDesc(value = "工商注册号")
        String regNumber;
        @FieldDesc(value = "状态")
        String regStatus;
        @FieldDesc(value = "暂无")
        String sourceFlag;
        @FieldDesc(value = "营业时间结束时间")
        Long toTime;
        @FieldDesc(value = "类型（1为公司，2为自然人）")
        Long type;
        @FieldDesc(value = "更新时间")
        String updateTimes;
        @FieldDesc(value = "更新时间")
        String updatetime;
        @FieldDesc(value = "网址列表")
        List<String> websiteList;
    }

    /**
     * 年报信息列表
     */
    @Data
    private static class DetailAnnualReportItem implements Item {
        @FieldDesc(value = "电子邮箱")
        String email;
        @FieldDesc(value = "年报id")
        Long id;
        @FieldDesc(value = "企业联系电话")
        String phoneNumber;
        @FieldDesc(value = "年份")
        String reportYear;
    }

    /**
     * 统计项
     */
    @Data
    private static class DetailAllCountItem implements Item {
        @FieldDesc(value = "年报数量")
        Long annuRepYear;
        @FieldDesc(value = "债券数量")
        Long bondCount;
        @FieldDesc(value = "分支机构数量")
        Long branchCount;
        @FieldDesc(value = "经营异常数量")
        Long comAbnoInfo;
        @FieldDesc(value = "变更信息数量")
        Long comChanInfoCount;
        @FieldDesc(value = "招投标数量")
        Long companyBidCount;
        @FieldDesc(value = "著作权数量")
        Long copyrightRegCount;
        @FieldDesc(value = "招聘数量")
        Long empCount;
        @FieldDesc(value = "投资数量")
        Long investCount;
        @FieldDesc(value = "股东数量")
        Long investorCount;
        @FieldDesc(value = "诉讼数量")
        Long lawSuitCount;
        @FieldDesc(value = "专利数量")
        Long patentCount;
        @FieldDesc(value = "高管数量")
        Long staffCount;
        @FieldDesc(value = "商标数量")
        Long tmCount;
    }

    /**
     * 分支机构
     */
    @Data
    private static class DetailBranchOrgItem implements Item {
        @FieldDesc(value = "公司id")
        Long id;
        @FieldDesc(value = "分支机构名")
        String name;
        @FieldDesc(value = "类型（1为公司，2为自然人）")
        Long type;
    }

    /**
     * 经营异常信息
     */
    @Data
    private static class DetailAbnormalItem implements Item {
        @FieldDesc(value = "列入日期")
        String putDate;
        @FieldDesc(value = "列入部门")
        String putDepartment;
        @FieldDesc(value = "列入经营异常名录原因")
        String putReason;
        @FieldDesc(value = "移出日期")
        String removeDate;
        @FieldDesc(value = "移出部门")
        String removeDepartment;
        @FieldDesc(value = "移出原因")
        String removeReason;
    }

    /**
     * 变更信息
     */
    @Data
    private static class DetailChangeItem implements Item {
        @FieldDesc(value = "变更项目")
        String changeItem;
        @FieldDesc(value = "变更时间")
        String changeTime;
        @FieldDesc(value = "变更后内容")
        String contentAfter;
        @FieldDesc(value = "变更前内容")
        String contentBefore;
    }

    /**
     * 投资公司
     */
    @Data
    private static class DetailInvestCorpItem implements Item {
        @FieldDesc(value = "投资金额（万元）")
        Double amount;
        @FieldDesc(value = "地区")
        String base;
        @FieldDesc(value = "行业")
        String category;
        @FieldDesc(value = "投资公司id")
        Long id;
        @FieldDesc(value = "法人姓名")
        String legalPersonName;
        @FieldDesc(value = "投资公司名")
        String name;
        @FieldDesc(value = "类型（1为公司，2为自然人）")
        Long type;
    }

    /**
     * 股东
     */
    @Data
    private static class DetailInvestorItem implements Item {
        @FieldDesc(value = "投资金额（万元）")
        Double amount;
        @FieldDesc(value = "股东id")
        Long id;
        @FieldDesc(value = "股东名")
        String name;
        @FieldDesc(value = "类型（1为公司，2为自然人）")
        Long type;
    }

    /**
     * 诉讼信息
     */
    @Data
    private static class DetailLawsuitItem implements Item {
        @FieldDesc(value = "案件号")
        String caseno;
        @FieldDesc(value = "法院")
        String court;
        @FieldDesc(value = "诉讼类型")
        String doctype;
        @FieldDesc(value = "提交时间")
        String submittime;
        @FieldDesc(value = "标题")
        String title;
        @FieldDesc(value = "原文地址")
        String url;
        @FieldDesc(value = "唯一id用于获取诉讼详情")
        String uuid;
    }

    /**
     * 高管
     */
    @Data
    private static class DetailSeniorStaffItem implements Item {
        @FieldDesc(value = "高管id")
        Long id;
        @FieldDesc(value = "高管姓名")
        String name;
        @FieldDesc(value = "类型（1为公司，2为自然人）")
        Long type;
        @FieldDesc(value = "职位")
        String[] typeJoin;
    }

    /**
     * 商标
     */
    @Data
    private static class DetailBrandItem implements Item {
        @FieldDesc(value = "商标名")
        String name;
        @FieldDesc(value = "商标图片地址")
        String url;
    }

}
