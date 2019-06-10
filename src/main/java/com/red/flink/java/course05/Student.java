package com.red.flink.java.course05;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * <pre>
 *  Version         Date            Author          Description
 * ------------------------------------------------------------
 *  1.0.0           2019/06/10     red        -
 * </pre>
 *
 * @author red
 * @version 1.0.0 2019-06-10 09:25
 * @since 1.0.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class Student {
	private int id;

	private String name;

	private int age;

}
