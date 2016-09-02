package com.acuumulo.playground;

import java.util.List;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;

public class AccAuths implements AccConstants{
	
	/**
	 * Generate Scan Auths for the provided list.
	 * @param auths List<String> | null
	 * @return Authorizations
	 */
	public static Authorizations generateScanAuths(List<String> auths){
		Authorizations scanAuths = new Authorizations();
		if (null != auths && !auths.isEmpty()) {
			scanAuths = new Authorizations((String[]) auths.toArray());
		}

		return scanAuths;
	}
	
	/**
	 * This is a very common test to be performed for auths strings (insert).
	 * Only checks for non-empty / null, not content of auths itself.
	 * @param auths String | null
	 * @return boolean
	 */
	public static boolean isAuth(String auths){
		return AccUtils.manipulate(auths, ManOp.EMPTY_TO_NULL) != null;
	}
	
	/**
	 * Generate a ColumnVisibility if {@link #isAuth(String)}.
	 * @param auths String | null
	 * @return ColumnVisiblity | null
	 */
	public static ColumnVisibility generateColVisIf(String auths){
		if (isAuth(auths))
			return new ColumnVisibility(auths);
		
		return null;
	}
}
