CREATE TABLE exchange_rates (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键ID',
    base_currency CHAR(3) NOT NULL COMMENT '基础货币(ISO 4217代码)',
    currencies JSON NOT NULL COMMENT '汇率数据集合(JSON格式)',
    data_provider VARCHAR(32) NOT NULL COMMENT '数据来源',
    data_updated_at TIMESTAMP NOT NULL COMMENT '汇率生效时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后更新时间',
    
    PRIMARY KEY (id),
    INDEX idx_data_updated_at (data_updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='JSON格式汇率缓存表';