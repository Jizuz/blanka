package com.blanka.cdc.autoconfigure;

import cn.hutool.core.util.StrUtil;
import com.blanka.cdc.autoconfigure.client.BlankaCdcClient;
import com.blanka.cdc.autoconfigure.route.TableRouteMatcher;
import com.blanka.cdc.autoconfigure.spi.BlankaRouteStrategy;
import com.blanka.cdc.autoconfigure.spi.InfraConfigProvider;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.util.Assert;

import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;

/**
 * blanka cdc 自动配置入口
 * <p>
 * 加载优先级（从高到低）：
 * <ol>
 *     <li>SPI InfraConfigProvider返回的基础设施地址</li>
 *     <li>yaml blanka.cdc显式配置</li>
 *     <li>项目全局RocketMQ配置（rocketmq.name-server、rocketmq.producer.group）自动拾取</li>
 *     <li>环境变量兜底：CANAL_SERVER_ADDR、CANAL_DESTINATION、ROCKETMQ_NAMESRV_ADDR</li>
 *     <li>starter内置默认值</li>
 * </ol>
 *
 * @author blanka
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(BlankaCdcProperties.class)
@ConditionalOnProperty(prefix = "blanka.cdc", name = "enable", havingValue = "true", matchIfMissing = true)
public class BlankaCdcAutoConfiguration {

    private static final Logger log = LoggerFactory.getLogger(BlankaCdcAutoConfiguration.class);

    @Resource
    private BlankaCdcProperties properties;

    @Resource
    private Environment environment;

    private final List<InfraConfigProvider> infraConfigProviders;

    public BlankaCdcAutoConfiguration(ObjectProvider<InfraConfigProvider> infraConfigProviders) {
        List<InfraConfigProvider> providers = infraConfigProviders.orderedStream().toList();
        this.infraConfigProviders = providers == null ? Collections.emptyList() : providers;
    }

    /**
     * 按优先级补全基础设施地址；参数缺失抛Assert快速失败
     */
    @PostConstruct
    public void fillInfraConfig() {
        // 1. SPI InfraConfigProvider：存在多个实现时取第一个；返回非空值覆盖原有配置，null/空字符串跳过
        if (!infraConfigProviders.isEmpty()) {
            InfraConfigProvider provider = infraConfigProviders.get(0);
            log.info("[blanka-cdc] InfraConfigProvider spi found: {}", provider.getClass().getName());
            if (StrUtil.isNotBlank(provider.getCanalServerAddr())) {
                properties.setCanalServerAddr(provider.getCanalServerAddr());
            }
            if (StrUtil.isNotBlank(provider.getDestination())) {
                properties.setDestination(provider.getDestination());
            }
            if (StrUtil.isNotBlank(provider.getNamesrvAddr())) {
                properties.setNamesrvAddr(provider.getNamesrvAddr());
            }
            if (StrUtil.isNotBlank(provider.getProducerGroup())) {
                properties.setProducerGroup(provider.getProducerGroup());
            }
        }

        // 3. 项目全局RocketMQ配置自动拾取；4. 环境变量兜底（yaml已有值则跳过）
        if (StrUtil.isBlank(properties.getCanalServerAddr())) {
            properties.setCanalServerAddr(environment.getProperty("CANAL_SERVER_ADDR"));
        }
        if (StrUtil.isBlank(properties.getDestination())) {
            properties.setDestination(environment.getProperty("CANAL_DESTINATION"));
        }
        if (StrUtil.isBlank(properties.getNamesrvAddr())) {
            properties.setNamesrvAddr(environment.getProperty("rocketmq.name-server"));
        }
        if (StrUtil.isBlank(properties.getNamesrvAddr())) {
            properties.setNamesrvAddr(environment.getProperty("ROCKETMQ_NAMESRV_ADDR"));
        }
        if (StrUtil.isBlank(properties.getProducerGroup())) {
            properties.setProducerGroup(environment.getProperty("rocketmq.producer.group"));
        }

        // 启动校验：不满足直接启动失败
        Assert.hasText(properties.getCanalServerAddr(),
                "[blanka-cdc] blanka.cdc.canal-server-addr is required: set yaml blanka.cdc.canal-server-addr"
                        + " or env CANAL_SERVER_ADDR");
        Assert.hasText(properties.getNamesrvAddr(),
                "[blanka-cdc] blanka.cdc.namesrv-addr is required: set yaml blanka.cdc.namesrv-addr"
                        + " or rocketmq.name-server or env ROCKETMQ_NAMESRV_ADDR");
        Assert.hasText(properties.getProducerGroup(),
                "[blanka-cdc] blanka.cdc.producer-group is required: set yaml blanka.cdc.producer-group"
                        + " or rocketmq.producer.group");

        log.info("[blanka-cdc] infra resolved, canalServerAddr={}, destination={}, namesrvAddr={}, producerGroup={}",
                properties.getCanalServerAddr(), properties.getDestination(),
                properties.getNamesrvAddr(), properties.getProducerGroup());
    }

    /**
     * SPI注册器：Java ServiceLoader加载 InfraConfigProvider / BlankaRouteStrategy 实现，
     * 注册为Spring bean，供list注入
     */
    @Bean
    public static BlankaCdcSpiRegistrar blankaCdcSpiRegistrar() {
        return new BlankaCdcSpiRegistrar();
    }

    @Bean
    @ConditionalOnMissingBean
    public TableRouteMatcher tableRouteMatcher(ObjectProvider<BlankaRouteStrategy> routeStrategies) {
        return new TableRouteMatcher(properties, routeStrategies.orderedStream().toList());
    }

    @Bean(destroyMethod = "shutdown")
    @ConditionalOnMissingBean(DefaultMQProducer.class)
    public DefaultMQProducer blankaDefaultMQProducer() throws MQClientException {
        DefaultMQProducer producer = new DefaultMQProducer(properties.getProducerGroup());
        producer.setNamesrvAddr(properties.getNamesrvAddr());
        producer.start();
        log.info("[blanka-cdc] DefaultMQProducer started, namesrvAddr={}, producerGroup={}",
                properties.getNamesrvAddr(), properties.getProducerGroup());
        return producer;
    }

    @Bean
    @ConditionalOnMissingBean
    public BlankaCdcClient blankaCdcClient(TableRouteMatcher tableRouteMatcher,
                                           ObjectProvider<DefaultMQProducer> producerProvider) {
        return new BlankaCdcClient(properties, tableRouteMatcher, producerProvider.getObject());
    }

    /**
     * SPI实现注册为Spring bean
     */
    static class BlankaCdcSpiRegistrar implements BeanDefinitionRegistryPostProcessor {

        @Override
        public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) {
            registerSpiImplementations(registry, InfraConfigProvider.class);
            registerSpiImplementations(registry, BlankaRouteStrategy.class);
        }

        @Override
        public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) {
            // no-op
        }

        private void registerSpiImplementations(BeanDefinitionRegistry registry, Class<?> spiInterface) {
            ServiceLoader<?> serviceLoader = ServiceLoader.load(spiInterface, spiInterface.getClassLoader());
            int count = 0;
            for (Object implementation : serviceLoader) {
                String beanName = spiInterface.getName() + "#" + implementation.getClass().getName();
                if (registry.containsBeanDefinition(beanName)) {
                    continue;
                }
                registry.registerBeanDefinition(beanName,
                        BeanDefinitionBuilder.genericBeanDefinition(implementation.getClass()).getBeanDefinition());
                count++;
                log.info("[blanka-cdc] spi implementation registered: {}", beanName);
            }
            if (count == 0) {
                log.info("[blanka-cdc] no spi implementation found for {}", spiInterface.getName());
            }
        }
    }
}
