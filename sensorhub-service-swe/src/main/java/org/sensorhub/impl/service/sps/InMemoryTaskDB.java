/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2012-2015 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sps;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.vast.ows.sps.StatusReport;
import org.vast.util.DateTime;


public class InMemoryTaskDB implements ITaskDB
{
	protected Map<String, ITask> taskTable;
	
	
	public InMemoryTaskDB()
	{
		this.taskTable = Collections.synchronizedMap(new HashMap<String, ITask>());
	}
	
	
	public void addTask(ITask task)
	{
	    taskTable.put(task.getID(), task);
	}


	public ITask getTask(String taskID)
	{
		return taskTable.get(taskID);
	}


	public StatusReport getTaskStatus(String taskID)
	{
	    ITask task = taskTable.get(taskID);
		if (task == null)
			return null;
		
		return task.getStatusReport();
	}


	public StatusReport getTaskStatusSince(String taskID, DateTime date)
	{
		// TODO Auto-generated method stub
		return null;
	}


	public void updateTaskStatus(StatusReport report)
	{
	    ITask task = taskTable.get(report.getTaskID());
		if (task == null)
			return;
		
		task.setStatusReport(report);
	}


	public void close()
	{	
	}
}
