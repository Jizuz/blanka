package com.blanka.cdc.autoconfigure.spi;

/**
 * 基础设施地址SPI
 * <p>
 * SPI加载方式：Java ServiceLoader，META-INF/services 注册实现类；starter自动注册到Spring上下文，list注入。
 * <ul>
 *     <li>存在多个实现时，取第一个；</li>
 *     <li>返回非空值覆盖原有配置；返回null/空字符串则跳过。</li>
 * </ul>
 *
 * @author blanka
 */
public interface InfraConfigProvider {

    /**
     * canal-server tcp地址，格式 host:port
     *
     * @return 地址，null/空字符串则跳过
     */
    String getCanalServerAddr();

    /**
     * canal destination
     *
     * @return destination，null/空字符串则跳过
     */
    String getDestination();

    /**
     * rocketmq nameserver地址
     *
     * @return 地址，null/空字符串则跳过
     */
    String getNamesrvAddr();

    /**
     * rocketmq producer group
     *
     * @return group，null/空字符串则跳过
     */
    String getProducerGroup();
}
