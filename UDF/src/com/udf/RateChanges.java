package com.udf;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class RateChanges extends EvalFunc<Tuple> {

	TupleFactory tupleFactory = TupleFactory.getInstance();
	
	@Override
	public Tuple exec(Tuple arg0) throws IOException {
	
		int increase = 0;
		int decrease = 0;
		int nochange = 0;
		Tuple result = tupleFactory.newTuple();
		DataBag inputBag = (DataBag)arg0.get(0);
		Iterator<Tuple> bagIterator = inputBag.iterator();
		
		while(bagIterator.hasNext()){
			Tuple currentTuple = bagIterator.next();
			if(((float)currentTuple.get(2) - (float)currentTuple.get(5)) > 0)
				increase++;
			else if(((float)currentTuple.get(2) - (float)currentTuple.get(5)) < 0)
				decrease++;
			else
				nochange++;
		}
		result.append(increase);
		result.append(decrease);
		result.append(nochange);
		return result;
	}
	
}
