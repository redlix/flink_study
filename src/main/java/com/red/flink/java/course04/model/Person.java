package com.red.flink.java.course04.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * <pre>
 *  Version         Date            Author          Description
 * ------------------------------------------------------------
 *  1.0.0           2019/06/04     red        -
 * </pre>
 *
 * @author red
 * @version 1.0.0 2019-06-04 09:44
 * @since 1.0.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Person {
	private String name;
	private int age;
	private String work;
}