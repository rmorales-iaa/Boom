//=============================================================================
//File: star.scala
//=============================================================================
/** It represents the information of stars of Boom!. See object declaration
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
package boom

//=============================================================================
// System import section
//=============================================================================
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

//=============================================================================
// User import section
//=============================================================================

//=============================================================================
// Class/Object implementation
//=============================================================================

//-------------------------------------------------------------------------
// Class declaration
//-------------------------------------------------------------------------

/**
 * Factory for [[boom.star]] instances. Creates the data scheme for stars used in BOOM!.
 * This scheme can be used to store data of starts in Parquet format
 */
object star
{
  //-------------------------------------------------------------------------
  // Variable declaration
  //-------------------------------------------------------------------------

    private val stelar_parameter = StructType(
       StructField("Mass",          DoubleType, false)::  //false indicate nullable
       StructField("Metallicity",   DoubleType, false)::
       StructField("Logg",          DoubleType, false)::  //logarithm of gravity
       StructField("Spectral_type", DoubleType, false)::
       StructField("Distance",      DoubleType, false)::
       StructField("Temperature",   DoubleType, false)::
       StructField("Radius",        DoubleType, false)::
       StructField("vsini",         DoubleType, false):: Nil
     )

     //-------------------------------------------------------------------------

    private val spectral_index = StructType(
       StructField("Spectral_index_01", DoubleType, false)::
       StructField("Spectral_index_02", DoubleType, false)::
       StructField("Spectral_index_03", DoubleType, false)::
       StructField("Spectral_index_04", DoubleType, false)::
       StructField("Spectral_index_05", DoubleType, false)::
       StructField("Spectral_index_06", DoubleType, false):: Nil
     )

     //-------------------------------------------------------------------------

     /** Data scheme for stars used in BOOM! */ 
    val schema = StructType(
       StructField("Name", StringType, false)::
       StructField("stelar_parameter", stelar_parameter, false)::
       StructField("spectral_index", spectral_index, false)::Nil
     )

     //-------------------------------------------------------------------------

     /** Translates a M start data from plain structure to the scheme defined for BOOM!*/
     def buildNestedRow(r:Row) : Row =
     {
       r match
       {
         //first plain data, later the structured scheme

         //plain data
         case Row(name:              String
                 ,mass:              Double
                 ,metallicity:       Double
                 ,logg:              Double
                 ,spectral_type:     Double
                 ,distance:          Double
                 ,temperature:       Double
                 ,radius:            Double
                 ,vsini:             Double
                 ,spectral_index_01: Double
                 ,spectral_index_02: Double
                 ,spectral_index_03: Double
                 ,spectral_index_04: Double
                 ,spectral_index_05: Double
                 ,spectral_index_06: Double) =>

                   //structured scheme of the plain data
                   Row(name:String,
                     Row(mass:          Double
                        ,metallicity:   Double
                        ,logg:          Double
                        ,spectral_type: Double
                        ,distance:      Double
                        ,temperature:   Double
                        ,radius:        Double
                        ,vsini:         Double)
                     ,Row( spectral_index_01: Double
                          ,spectral_index_02: Double
                          ,spectral_index_03: Double
                          ,spectral_index_04: Double
                          ,spectral_index_05: Double
                          ,spectral_index_06: Double)
                     )
       }
     }

     //-------------------------------------------------------------------------
     //-------------------------------------------------------------------------
} //end of object 'Star'

//-------------------------------------------------------------------------
//-------------------------------------------------------------------------

//=============================================================================
// End of 'star.scala' file
//=============================================================================
