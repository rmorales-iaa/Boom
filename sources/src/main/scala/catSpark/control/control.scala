//=============================================================================
//File: control.scala
//=============================================================================
/** It implements new control structures for catSpark. See object declaration
 *  @author  Rafael Morales Muñoz
 *  @mail    rmorales.iaa.es
 *  @version 1.0
 *  @date    9 Nov 2016
 *  @history None
 */
//=============================================================================
//=============================================================================
// Package section
//=============================================================================
package catSpark.control

//=============================================================================
// System import section
//=============================================================================
import scala.util.{Try, Success, Failure}

//=============================================================================
// User import section
//=============================================================================
import catSpark.logger.myLogger

//=============================================================================
// Class/Object implementation
//=============================================================================
//=============================================================================
/** Useful control structures */
object control {

  //-------------------------------------------------------------------------
  /** Uses and closes and object
   * @param obj Object that must has defined the method 'def close(): Unit'
   * @param f a function that transforms type A in type B
   * @return Object of type B when success or Failure in other case
   */
  //-------------------------------------------------------------------------
  def call_and_close[A <: { def close(): Unit }, B](obj: A)(f: A => B): Try[B] =
  {
    Try(f(obj)) match //call function with the parameter. It is executed inside the apply method of the Try object
    {
      case Success(v) =>
        obj.close() //run close method on the parameter in any case
        Success(v)

      case Failure(e) =>
        obj.close() //run close method on the parameter in any case
        myLogger.error("Error in 'Control.use_and_close' method. Funciton: '"+f.toString()+"' parameter: '"+obj.toString()+"'")
        Failure(e)
    }
  }
  //-------------------------------------------------------------------------
  //-------------------------------------------------------------------------
} //End of object 'control'

//=============================================================================
// End of 'control.scala' file
//=============================================================================
