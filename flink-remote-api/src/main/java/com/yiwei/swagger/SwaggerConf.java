package com.yiwei.swagger;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

/**
 * @author yiwei  2020/4/11
 */
@Configuration
@EnableSwagger2
public class SwaggerConf {
    @Bean
    public Docket docket() {
        return new Docket(DocumentationType.SWAGGER_2).groupName("swagger接口文档")
                .apiInfo(new ApiInfoBuilder().title("swagger接口文档").version("1.0").build())
                .select()
                .apis(RequestHandlerSelectors.basePackage("com.yiwei.controller"))
                .paths(PathSelectors.any())
                .build();
    }
}
