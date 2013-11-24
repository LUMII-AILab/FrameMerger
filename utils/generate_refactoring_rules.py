#!/usr/bin/env python
# -*- coding: utf8 -*-


# def relationship_role_to_property_assertion(relationship_role_name, property_name):
#     return '''
# [] a rule:SPARQLRule ;
#    rule:content """
#         PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
#         PREFIX meta: <http://lumii.lv/ontologies/LETA_Frames#>
#         PREFIX extended: <http://lumii.lv/ontologies/LETA_Frames_Extended#>
#         PREFIX data: <http://lumii.lv/rdf_data/LETA_Frames/>
#      IF {
#         ?frame rdf:type meta:Personal_relationship .
#         ?frame meta:Relationship ?relationship .

#         ?relationship rdfs:label "%(relationship_role_name)s" .

#         ?frame meta:Partner_1 ?partner1 .
#         ?frame meta:Partner_2 ?partner2 .
#      }
#      THEN {
#         ?partner2 meta:%(property_name)s ?partner1 .
#      }
#    """.
# ''' % {"relationship_role_name":relationship_role_name, "property_name":property_name}

def relationship_role_to_property_assertion_stardog(relationship_role_name, property_name):
    return '''
[] a rule:SPARQLRule ;
   rule:content """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX meta: <http://lumii.lv/ontologies/LETA_Frames#>
        PREFIX extended: <http://lumii.lv/ontologies/LETA_Frames_Extended#>
        PREFIX data: <http://lumii.lv/rdf_data/LETA_Frames/>
     IF {
        ?frame rdf:type meta:Personal_relationship .
        ?frame meta:Relationship data:%(relationship_role_name)s .

        ?frame meta:Partner_1 ?partner1 .
        ?frame meta:Partner_2 ?partner2 .
     }
     THEN {
        ?partner2 extended:%(property_name)s ?partner1 .
     }
   """.
''' % {"relationship_role_name":relationship_role_name, "property_name":property_name}

def relationship_role_to_property_assertion_functional_syntax(relationship_role_name, property_name):
    return '''
    DLSafeRule(
        Body(
            ClassAtom( meta:Personal_relationship Variable(var:frame)) 
            ObjectPropertyAtom( meta:Relationship Variable(var:frame) data:%(relationship_role_name)s)

            ObjectPropertyAtom( meta:Partner_1 Variable(var:frame) Variable(var:partner1) ) 
            ObjectPropertyAtom( meta:Partner_2 Variable(var:frame) Variable(var:partner2) )
        )
        Head(
            ObjectPropertyAtom( extended:%(property_name)s Variable(var:partner2) Variable(var:partner1) ) 
        )
    )
''' % {"relationship_role_name":relationship_role_name, "property_name":property_name}


def rules_for_releationship_refactoring(relationship_role_to_property_assertion):
    # mapping = [
    #     (u"sieva", "wife"),
    #     (u"dēls", "son"),
    #     (u"meita", "daughter"),
    #     (u"tēvs", "father"),
    #     (u"māte", "mother"),
    #     (u"vīrs", "husband"),
    #     (u"brālis", "brother"),
    #     (u"māsa", "sister")]

    mapping = [
        ("entity_15", "wife"),
        ("entity_1401396", "daughter"),
        ("entity_1400229", "son"),
        ("entity_1401484", "father"),
        ("entity_14", "mother"),
        ("entity_1484408", "son"),
        ("entity_1400674", "husband"),
        ("entity_1400738", "brother"),
        ("entity_1405269", "sister")]


    return "\n\n".join([relationship_role_to_property_assertion(role, prop) for (role, prop) in mapping])

def gen_rules_in_stardog_syntax():
    return """
PREFIX rule: <tag:stardog:api:rule:>

%(relationship_role_to_property_assertions)s
""" % {"relationship_role_to_property_assertions" : rules_for_releationship_refactoring(relationship_role_to_property_assertion_stardog)}


def gen_rules_in_owl_functional_syntax():
    return """
Prefix(var:=<urn:swrl#>)
Prefix(meta:=<http://lumii.lv/ontologies/LETA_Frames#>)
Prefix(extended:=<http://lumii.lv/ontologies/LETA_Frames_Extended#>)
Prefix(data:=<http://lumii.lv/rdf_data/LETA_Frames/>)

Ontology(<http://lumii.lv/ontologies/LETA_Frames_Rules#>
    %(relationship_role_to_property_assertions)s
    
)""" % {"relationship_role_to_property_assertions" : rules_for_releationship_refactoring(relationship_role_to_property_assertion_functional_syntax)}
    


def main():
    rules = gen_rules_in_owl_functional_syntax()
    output_file_name = "./output/refactoring_rules.owl"

    print "Saving refactoring rules to [%s]" % (output_file_name,)
    with open(output_file_name, "w") as outf:
        outf.write(rules.encode('utf8'))




if __name__ == "__main__":
    main()