package com.blanka.cdc.autoconfigure;

import org.springframework.context.annotation.Import;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * blanka cdc 启用注解（可选）
 * <p>
 * 不添加注解 + 配置enable=true，组件依然自动装配；
 * 使用场景：多CDC组件共存时，业务显式开启。
 *
 * @author blanka
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(BlankaCdcAutoConfiguration.class)
public @interface EnableBlankaCdc {
}
