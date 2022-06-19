package com.itheima.elasticsearchdemo.mapper;


import com.itheima.elasticsearchdemo.domain.Goods;
import org.apache.ibatis.annotations.Mapper;
import org.springframework.stereotype.Repository;

import java.util.List;

@Mapper
@Repository
public interface GoodsMapper {

    /**
     * 查询所有
     */
    public List<Goods> findAll();
}
