package com.demo.余弦相似度.test;


import com.demo.余弦相似度.util.CosineSimilarity;

public class Similarity {


    public static final String content1 = "今天小小和爸爸一起去摘草莓，小小说今天的草莓特别的酸，而且特别的小，关键价格还贵";

    public static final String content2 = "今天小小和妈妈一起去草原里采草莓，今天的草莓味道特别好，而且价格还挺实惠的";

    public static final String content3 = "8月10日，美国总统拜登在白宫进行消费价格指数报告。在演讲过程中，拜登再次出现尴尬口误。他提到了正在推进的《降低通胀法案》，敦促国会尽快通过该法案。然而，他却将“防止美国在通胀方面变得更糟”说成了“防止美国在通胀方面变得更好”。" ;
    public static final String content4 = "美国总统拜登在白宫进行消费价格指数报告。在演讲过程中，拜登再次出现尴尬口误。他提到了正在推进的《降低通胀法案》，敦促国会尽快通过该法案。然而，他却将“防止美国在通胀方面变得更糟”说成了“防止美国在通胀方面变得更好”。" ;


    public static void main(String[] args) {
        System.out.println("利用余弦相似度计算文本的相似度：适用于文本、图像、视频等" );
        getSimilarity(content1, content2);
        getSimilarity(content3, content4);
    }

    public static void getSimilarity(String content1, String content2) {
        System.out.println("利用余弦相似度计算文本的相似度：适用于文本、图像、视频等" );

        System.out.println("文本-1：" + content1);
        System.out.println("文本-2：" + content2);

        double score = CosineSimilarity.getSimilarity(content1, content2);
        System.out.println("相似度：" + score);
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
    }
}
