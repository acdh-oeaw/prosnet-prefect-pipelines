from collections import ChainMap
import enum
from itertools import chain
import os
from pathlib import Path
import shutil
import git
from prefect import flow, get_run_logger, task, unmapped
from prefect.concurrency.sync import rate_limit
from copy import deepcopy
from datetime import datetime, timedelta
import requests
from rdflib import Graph, Literal, RDF, Namespace, URIRef, Dataset
from rdflib.namespace import RDFS, XSD
from typing import Any, Tuple
from pydantic import BaseModel, DirectoryPath, Extra, Field, HttpUrl

# from push_rdf_file_to_github import push_data_to_repo_flow
# from push_rdf_file_to_github import Params as ParamsPush
from prefect.concurrency.sync import rate_limit


def convert_timedelta(duration):
    days, seconds = duration.days, duration.seconds
    hours = days * 24 + seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    return hours, minutes, seconds


def create_time_span_tripels(kind, event_node, obj):
    global g
    if kind == "start":
        if obj["start_date_written"] is not None:
            if len(obj[f"start_date_written"]) > 0:
                label_date = obj[f"start_date_written"]
                if obj["end_date_written"] is not None:
                    if len(obj["end_date_written"]) > 0:
                        label_date += f" - {obj['end_date_written']}"
                g.add((event_node, rdfs.label, Literal(label_date)))
    if len(obj[f"{kind}_date_written"]) == 4:
        # check whether only a year has bin given for the start date and add according nodes
        if kind == "start":
            g.add(
                (
                    event_node,
                    crm.P82a_begin_of_the_begin,
                    (
                        Literal(
                            f"{obj[f'{kind}_date_written']}-01-01T00:00:00",
                            datatype=XSD.dateTime,
                        )
                    ),
                )
            )
            g.add(
                (
                    event_node,
                    crm.P81a_end_of_the_begin,
                    (
                        Literal(
                            f"{obj[f'{kind}_date_written']}-12-31T23:59:59",
                            datatype=XSD.dateTime,
                        )
                    ),
                )
            )
        elif kind == "end":
            g.add(
                (
                    event_node,
                    crm.P82b_end_of_the_end,
                    (
                        Literal(
                            f"{obj[f'{kind}_date_written']}-12-31T23:59:59",
                            datatype=XSD.dateTime,
                        )
                    ),
                )
            )
            g.add(
                (
                    event_node,
                    crm.P81b_begin_of_the_end,
                    (
                        Literal(
                            f"{obj[f'{kind}_date_written']}-01-01T00:00:00",
                            datatype=XSD.dateTime,
                        )
                    ),
                )
            )
    else:
        if kind == "start":
            g.add(
                (
                    event_node,
                    crm.P82a_begin_of_the_begin,
                    (Literal(f"{obj[f'{kind}_date']}T00:00:00", datatype=XSD.dateTime)),
                )
            )
        elif kind == "end":
            g.add(
                (
                    event_node,
                    crm.P82b_end_of_the_end,
                    (Literal(f"{obj[f'{kind}_date']}T23:59:59", datatype=XSD.dateTime)),
                )
            )
    g.add((event_node, RDF.type, crm["E52_Time-Span"]))
    return True


@task(tags=["render-rdflib"], timeout_seconds=60)
def render_personplace_relation(rel):
    """renders personplace relation as RDF graph

    Args:
        pers_uri (_type_): _description_
        rel (_type_): _description_
        g (_type_): _description_
    """
    # prepare nodes
    global g
    place = None
    place_uri = idmapis[f"place.{rel['related_place']['id']}"]
    if rel["relation_type"]["id"] == 595:
        # define serialization for "person born in place relations"
        g.add(
            (
                idmapis[f"birthevent.{rel['related_person']['id']}"],
                crm.P7_took_place_at,
                place_uri,
            )
        )
        rel_rendered = True
    elif rel["relation_type"]["id"] == 596:
        # define serialization for "person born in place relations"
        g.add(
            (
                idmapis[f"deathevent.{rel['related_person']['id']}"],
                crm.P7_took_place_at,
                place_uri,
            )
        )
        rel_rendered = True
    else:
        event_uri = idmapis[f"event.personplace.{rel['id']}"]
        if (event_uri, None, None) not in g:
            render_event(rel, "personplace", event_uri)
        g.add((event_uri, crm.P7_took_place_at, place_uri))
        rel_rendered = True

    if (
        rel_rendered
        and (place_uri, None, None) not in g
        and rel["related_place"]["id"] not in glob_list_entities["place"]
    ):
        place = rel["related_place"]["id"]
        glob_list_entities["place"].append(rel["related_place"]["id"])
    return True


@task(tags=["render-rdflib"], timeout_seconds=60)
def render_personperson_relation(
    rel,
    family_relations=[
        5870,  # war Bruder von
        5871,  # war Schwester von
        5741,  # family member
        5414,  # war Kind von
        5413,  # war Elternteil von
        5412,  # war verwandt
        5411,  # war verheiratet
    ],
):
    """renders personperson relation as RDF graph

    Args:
        pers_uri (_type_): _description_
        rel (_type_): _description_
        g (_type_): _description_http://localhost:8000/apis/api/entities/person/
    """
    # prepare nodes
    global g
    logger = get_run_logger()
    if isinstance(rel, list):
        if len(rel) == 0:
            logger.warning("No person-person relations found skipping rest of task")
            return None
            # return Exception("No person-person relations found skipping")
    if rel["relation_type"]["label"] == "undefined":
        logger.warning("Relation type is undefined, skipping rest of task")
        # return Exception("Relation type is undefined, skipping")
    person = None
    pers_uri = idmapis[f"personproxy.{rel['related_personA']['id']}"]
    n_rel_type = idmapis[f"personrelation.{rel['id']}"]
    n_relationtype = idmrelations[f"{rel['relation_type']['id']}"]
    if rel["relation_type"]["id"] in family_relations:
        g.add((pers_uri, bioc.has_family_relation, n_rel_type))
    else:
        g.add((pers_uri, bioc.has_person_relation, n_rel_type))
    g.add((n_rel_type, RDF.type, n_relationtype))
    g.add(
        (n_rel_type, RDFS.label, Literal(f"{rel['relation_type']['label']}", lang="de"))
    )
    g.add(
        (
            idmapis[f"personproxy.{rel['related_personB']['id']}"],
            bioc.bearer_of,
            n_rel_type,
        )
    )
    if rel["related_personB"]["id"] not in glob_list_entities["person"]:
        glob_list_entities["person"].append(rel["related_personB"]["id"])
    # g.add(n_rel_type, bioc.bearer_of, (URIRef(
    #    f"{idmapis}personproxy/{rel['related_personB']['id']}")))
    # TODO: add hiarachy of person relations
    if rel["relation_type"] is not None:
        if rel["relation_type"]["parent_id"] is not None:
            g.add(
                (
                    n_relationtype,
                    RDFS.subClassOf,
                    idmrelations[f"{rel['relation_type']['parent_id']}"],
                )
            )
            if rel["relation_type"]["id"] in family_relations:
                g.add(
                    (
                        idmrelations[f"{rel['relation_type']['parent_id']}"],
                        RDFS.subClassOf,
                        bioc.Family_Relationship_Role,
                    )
                )
            else:
                g.add(
                    (
                        idmrelations[f"{rel['relation_type']['parent_id']}"],
                        RDFS.subClassOf,
                        bioc.Person_Relationship_Role,
                    )
                )
        else:
            if rel["relation_type"]["id"] in family_relations:
                g.add(
                    (
                        idmrelations[f"{rel['relation_type']['id']}"],
                        RDFS.subClassOf,
                        bioc.Family_Relationship_Role,
                    )
                )
            else:
                g.add(
                    (
                        idmrelations[f"{rel['relation_type']['id']}"],
                        RDFS.subClassOf,
                        bioc.Person_Relationship_Role,
                    )
                )

    else:
        if rel["relation_type"]["id"] in family_relations:
            g.add((n_relationtype, RDFS.subClassOf, bioc.Family_Relationship_Role))
        else:
            g.add((n_relationtype, RDFS.subClassOf, bioc.Person_Relationship_Role))
    logger.info(f" personpersonrelation serialized for: {rel['related_personA']['id']}")
    # return g
    # group which is part of this relation
    return True


@task(tags=["render-rdflib"], timeout_seconds=60)
def render_personrole_from_relation(rel):
    """renders personrole as RDF graph

    Args:
        pers_uri (_type_): _description_
        role (_type_): _description_
        g (_type_): _description_
    """
    # prepare nodes
    global g
    logger = get_run_logger()
    if (
        idmapis[f"personrole.{rel['relation_type']['id']}"],
        None,
        None,
    ) not in g:
        for k in rel.keys():
            if k.startswith("related_"):
                if k.split("_")[1] != "person":
                    role = rel["relation_type"]
                    second_entity = k.split("_")[1]
    else:
        logger.info("personrole already in graph")
        return None
    label = "label"
    if "label" not in role:
        label = "name"
    parent = "parent_id"
    if "parent_id" not in role:
        parent = "parent_class"
    n_role = idmapis[f"personrole.{role['id']}"]
    g.add((n_role, RDFS.label, Literal(f"{role[label]}", lang="de")))
    if role[parent] is not None:
        if "parent_id" in role:
            p_id = role["parent_id"]
        else:
            p_id = role["parent_class"]["id"]
        if (idmapis[f"personrole.{p_id}"], None, None) not in g:
            if f"person{second_entity}relation" not in glob_list_vocabs:
                glob_list_vocabs[f"person{second_entity}relation"] = []
            glob_list_vocabs[f"person{second_entity}relation"].append(p_id)
            return True
    else:
        g.add((n_role, RDF.type, bioc.Actor_Role))
    return True


@task(tags=["render-rdflib"], timeout_seconds=60)
def render_personrole(role):
    global g
    logger = get_run_logger()
    if role is None:
        logger.warning("No role given")
        return None
    if (idmapis[f"personrole.{role['id']}"], None, None) in g:
        logger.info("personrole already in graph")
        return True
    n_role = idmapis[f"personrole.{role['id']}"]
    g.add((n_role, RDFS.label, Literal(f"{role['name']}", lang="de")))
    if role["parent_class"] is not None:
        if (
            idmapis[f"personrole.{role['parent_class']['id']}"],
            None,
            None,
        ) not in g:
            logger.warning(f"parent role not in graph")
            if role["url"].split("/")[-3] not in glob_list_vocabs:
                glob_list_vocabs[role["url"].split("/")[-3]] = []
            glob_list_vocabs[role["url"].split("/")[-3]].append(
                role["parent_class"]["id"]
            )
            return True
    else:
        g.add((n_role, RDF.type, bioc.Actor_Role))
        return True


@task(tags=["render-rdflib"], timeout_seconds=60)
def render_personinstitution_relation(rel: dict) -> Tuple[Graph, Any]:
    global g
    logger = get_run_logger()
    pers_uri = idmapis[f"personproxy.{rel['related_person']['id']}"]
    inst = None
    # connect personproxy and institutions with grouprelationship
    n_rel_type = idmapis[f"grouprelation.{rel['id']}"]
    g.add((pers_uri, bioc.has_group_relation, n_rel_type))
    # Person has a specific group relation
    g.add(
        (
            n_rel_type,
            RDF.type,
            idmapis[f"grouprole.{rel['relation_type']['id']}"],
        )
    )
    # define type of grouprelation
    if rel["relation_type"]["parent_id"] is not None:
        # if the relationtype has a superclass, it is added here
        g.add(
            (
                idmapis[f"grouprole.{rel['relation_type']['id']}"],
                rdfs.subClassOf,
                idmapis[f"grouprole.{rel['relation_type']['parent_id']}"],
            )
        )
    g.add((n_rel_type, rdfs.label, Literal(rel["relation_type"]["label"], lang="de")))
    # add label to relationtype
    g.add(
        (
            n_rel_type,
            bioc.inheres_in,
            idmapis[f"groupproxy.{rel['related_institution']['id']}"],
        )
    )
    # g.add((URIRef(
    #    f"{idmapis}groupproxy/{rel['related_institution']['id']}"), bioc.bearer_of, n_rel_type))
    # group which is part of this relation
    g.add((idmapis[f"career.{rel['id']}"], RDF.type, idmcore.Career))
    # add career event of type idmcore:career
    g.add(
        (
            idmcore.Career,
            rdfs.subClassOf,
            crm.E5_Event,
        )
    )
    g.add(
        (
            idmapis[f"career.{rel['id']}"],
            rdfs.label,
            Literal(
                f"{rel['related_person']['label']} {rel['relation_type']['label']} {rel['related_institution']['label']}",
                lang="de",
            ),
        )
    )
    # label for career event
    g.add(
        (
            idmapis[f"career.{rel['id']}"],
            bioc.had_participant_in_role,
            idmapis[f"personrole.{rel['id']}.{rel['related_person']['id']}"],
        )
    )
    # role of participating person in the career event
    g.add(
        (
            idmapis[f"personproxy.{rel['related_person']['id']}"],
            bioc.bearer_of,
            idmapis[f"personrole.{rel['id']}.{rel['related_person']['id']}"],
        )
    )
    g.add(
        (
            idmapis[f"personrole.{rel['id']}.{rel['related_person']['id']}"],
            RDF.type,
            idmapis[f"personrole.{rel['relation_type']['id']}"],
        )
    )
    if rel["relation_type"]["parent_id"] is not None:
        g.add(
            (
                idmapis[f"personrole.{rel['relation_type']['id']}"],
                RDF.type,
                idmapis[f"personrole.{rel['relation_type']['parent_id']}"],
            )
        )
    # person which inheres this role
    g.add(
        (
            idmapis[f"career.{rel['id']}"],
            bioc.had_participant_in_role,
            idmapis[f"grouprole.{rel['id']}.{rel['related_institution']['id']}"],
        )
    )
    # role of institution/ group in the career event
    g.add(
        (
            idmapis[f"grouprole.{rel['id']}.{rel['related_institution']['id']}"],
            RDF.type,
            bioc.Group_Relationship_Role,
        )
    )
    g.add(
        (
            idmapis[f"grouprole.{rel['id']}.{rel['related_institution']['id']}"],
            bioc.inheres_in,
            idmapis[f"groupproxy.{rel['related_institution']['id']}"],
        )
    )
    # role which inheres the institution/ group
    if (
        idmapis[f"groupproxy.{rel['related_institution']['id']}"],
        None,
        None,
    ) not in g:
        if rel["related_institution"]["id"] not in glob_list_entities["institution"]:
            inst = rel["related_institution"]["id"]
            glob_list_entities["institution"].append(rel["related_institution"]["id"])
    if rel["start_date"] is not None or rel["end_date"] is not None:
        g.add(
            (
                idmapis[f"career.{rel['id']}"],
                URIRef(f"{crm}P4_has_time-span"),
                idmapis[f"career.timespan.{rel['id']}"],
            )
        )
    for rel_plcs in g.objects(
        idmapis[f"groupproxy.{rel['related_institution']['id']}"],
        crm.P74_has_current_or_former_residence,
    ):
        g.add((idmapis[f"career.{rel['id']}"], crm.P7_took_place_at, rel_plcs))
    logger.info(
        f" personinstitutionrelation serialized for: {rel['related_person']['id']}"
    )
    if rel["start_date"] is not None:
        create_time_span_tripels("start", idmapis[f"career.timespan.{rel['id']}"], rel)
    if rel["end_date"] is not None:
        create_time_span_tripels("end", idmapis[f"career.timespan.{rel['id']}"], rel)
    """     if (rel['start_date'] is not None) and (rel['end_date'] is not None):
        g.add((URIRef(f"{idmapis}career/timespan/{rel['id']}"), crm.P82a_begin_of_the_begin, (Literal(
            rel['start_date']+'T00:00:00', datatype=XSD.dateTime))))
        g.add((URIRef(f"{idmapis}career/timespan/{rel['id']}"), crm.P82b_end_of_the_end, (Literal(
            rel['end_date']+'T23:59:59', datatype=XSD.dateTime))))
        g.add((URIRef(f"{idmapis}career/timespan/{rel['id']}"), rdfs.label, Literal(
            rel['start_date_written'])+' - ' + rel['end_date_written']))
    elif ((rel['start_date'] is not None) and (rel['end_date'] is not None)):
        g.add((URIRef(f"{idmapis}career/timespan/{rel['id']}"), crm.P82a_begin_of_the_begin, (Literal(
            rel['start_date']+'T00:00:00', datatype=XSD.dateTime))))
        g.add((URIRef(
            f"{idmapis}career/timespan/{rel['id']}"), rdfs.label, Literal(rel['start_date_written'])))
    elif ((rel['start_date'] is not None) and (rel['end_date'] is not None)):
        g.add((URIRef(f"{idmapis}career/timespan/{rel['id']}"), crm.P82b_end_of_the_end, (Literal(
            rel['end_date']+'T23:59:59', datatype=XSD.dateTime))))
        g.add((URIRef(f"{idmapis}career/timespan/{rel['id']}"), rdfs.label, Literal(
            'time-span end:' + rel['end_date_written']))) """
    return True


@task(tags=["render-rdflib"], timeout_seconds=60)
def render_person(person):
    """renders person object as RDF graph

    Args:
        person (_type_): _description_
        g (_type_): _description_
    """
    # pers_uri = URIRef(idmapis + f"personproxy/{person['id']}")
    global g
    pers_uri = idmapis[f"personproxy.{person['id']}"]
    if (pers_uri, None, None) in g:
        return True
    g.add((pers_uri, RDF.type, crm.E21_Person))
    g.add((pers_uri, RDF.type, idmcore.Person_Proxy))
    g.add(
        (
            pers_uri,
            RDFS.label,
            Literal(f"{person['first_name']} {person['name']}", lang="de"),
        )
    )
    # define that individual in APIS named graph and APIS entity are the same
    g.add((pers_uri, owl.sameAs, URIRef(f"{base_uri}/entity/{person['id']}")))
    # add sameAs
    # add appellations
    node_main_appellation = idmapis[f"appellation.label.{person['id']}"]
    g.add((node_main_appellation, RDF.type, crm.E33_E41_Linguistic_Appellation))
    g.add(
        (
            node_main_appellation,
            RDFS.label,
            Literal(
                f"{person['name'] if person['name'] is not None else '-'}, {person['first_name'] if person['first_name'] is not None else '-'}",
                lang="de",
            ),
        )
    )
    g.add((pers_uri, crm.P1_is_identified_by, node_main_appellation))
    if person["first_name"] is not None:
        node_first_name_appellation = idmapis[f"appellation.first_name.{person['id']}"]
        g.add(
            (node_first_name_appellation, RDF.type, crm.E33_E41_Linguistic_Appellation)
        )
        g.add(
            (
                node_first_name_appellation,
                RDFS.label,
                Literal(person["first_name"], lang="de"),
            )
        )
        g.add(
            (node_main_appellation, crm.P148_has_component, node_first_name_appellation)
        )
    if person["name"] is not None:
        node_last_name_appellation = idmapis[f"appellation.last_name.{person['id']}"]
        g.add(
            (node_last_name_appellation, RDF.type, crm.E33_E41_Linguistic_Appellation)
        )
        g.add(
            (node_last_name_appellation, RDFS.label, Literal(person["name"], lang="de"))
        )
        g.add(
            (node_main_appellation, crm.P148_has_component, node_last_name_appellation)
        )
    if person["start_date"] is not None:
        node_birth_event = idmapis[f"birthevent.{person['id']}"]
        node_role = idmapis[f"born_person.{person['id']}"]
        node_role_class = idmrole[f"born_person"]
        node_time_span = idmapis[f"birth.timespan.{person['id']}"]
        g.add((node_role, bioc.inheres_in, pers_uri))
        g.add((node_role, RDF.type, node_role_class))
        g.add((node_role_class, rdfs.subClassOf, bioc.Event_Role))
        g.add((node_birth_event, bioc.had_participant_in_role, node_role))
        g.add((node_birth_event, RDF.type, crm.E67_Birth))
        g.add(
            (
                node_birth_event,
                RDFS.label,
                Literal(f"Birth of {person['first_name']} {person['name']}"),
            )
        )
        g.add((node_birth_event, crm["P4_has_time-span"], node_time_span))
        g.add((node_birth_event, crm.P98_brought_into_life, pers_uri))
        create_time_span_tripels("start", node_time_span, person)
    if person["end_date"] is not None:
        node_death_event = idmapis[f"deathevent.{person['id']}"]
        node_role = idmapis[f"deceased_person.{person['id']}"]
        node_role_class = idmrole["deceased_person"]
        node_time_span = idmapis[f"death.timespan.{person['id']}"]
        g.add((node_role, bioc.inheres_in, pers_uri))
        g.add((node_role, RDF.type, node_role_class))
        g.add((node_role_class, rdfs.subClassOf, bioc.Event_Role))
        g.add((node_death_event, bioc.had_participant_in_role, node_role))
        g.add((node_death_event, RDF.type, crm.E69_Death))
        g.add(
            (
                node_death_event,
                RDFS.label,
                Literal(f"Death of {person['first_name']} {person['name']}", lang="de"),
            )
        )
        g.add((node_death_event, crm["P4_has_time-span"], node_time_span))
        g.add((node_death_event, crm.P100_was_death_of, pers_uri))
        create_time_span_tripels("end", node_time_span, person)
    for prof in person["profession"]:
        prof_node = idmapis[f"occupation.{prof['id']}"]
        g.add((pers_uri, bioc.has_occupation, prof_node))
        g.add((prof_node, rdfs.label, Literal(prof["label"], lang="de")))
        if prof["parent_id"] is not None:
            parent_prof_node = idmapis[f"occupation.{prof['parent_id']}"]
            g.add((prof_node, rdfs.subClassOf, parent_prof_node))
            g.add((prof_node, rdfs.subClassOf, bioc.Occupation))
        else:
            g.add((prof_node, rdfs.subClassOf, bioc.Occupation))
    if person["gender"] is not None:
        if len(person["gender"]) > 0:
            g.add((pers_uri, bioc.has_gender, bioc[person["gender"].capitalize()]))
    for uri in person["sameAs"]:
        g.add((pers_uri, owl.sameAs, URIRef(uri)))
    if "text" in person:
        if len(person["text"]) > 1:
            g.add(
                (
                    pers_uri,
                    idmcore.bio_link,
                    idmapis[f"text.{person['id']}.bio"],
                )
            )
            g.add(
                (
                    idmapis[f"text.{person['id']}.bio"],
                    idmcore.full_bio_link,
                    URIRef(person["text"][0]["url"]),
                )
            )
            g.add(
                (
                    idmapis[f"text.{person['id']}.bio"],
                    idmcore.short_bio_link,
                    URIRef(person["text"][1]["url"]),
                )
            )
    # add occupations

    #    person_rel = await get_person_relations(person['id'], kinds=['personinstitution', 'personperson', 'personplace'])
    # tasks = []
    # for rel_type, rel_list in person_rel.items():
    #     if rel_type == 'personinstitution':
    #         for rel in rel_list:
    #             tasks.append(asyncio.create_task(
    #                 render_personinstitution_relation(pers_uri, rel, g)))
    #     elif rel_type == 'personperson':
    #         for rel in rel_list:
    #             tasks.append(asyncio.create_task(
    #                 render_personperson_relation(pers_uri, rel, g)))
    #     elif rel_type == 'personplace':
    #         for rel in rel_list:
    #             tasks.append(asyncio.create_task(
    #                 render_personplace_relation(pers_uri, rel, g)))
    #   await asyncio.gather(*tasks)
    return True


@task(tags=["render-rdflib"], timeout_seconds=60)
def render_organizationplace_relation(rel):
    global g
    place = None
    node_org = idmapis[f"groupproxy.{rel['related_institution']['id']}"]
    if (
        idmapis[f"place.{rel['related_place']['id']}"],
        None,
        None,
    ) not in g and rel["related_place"]["id"] not in glob_list_entities["place"]:
        place = rel["related_place"]["id"]
        glob_list_entities["place"].append(place)
    g.add(
        (
            node_org,
            crm.P74_has_current_or_former_residence,
            idmapis[f"place.{rel['related_place']['id']}"],
        )
    )
    return True


@task(tags=["render-rdflib"], timeout_seconds=60)
def render_organization(organization):
    """renders organization object as RDF graph

    Args:
        organization (_type_): _description_
        g (_type_): _description_
    """
    global g
    #    res = await get_entity(organization, "institution")
    #    res_relations = await get_organization_relations(organization, ["institutionplace"])
    # setup basic nodes
    node_org = idmapis[f"groupproxy.{organization['id']}"]
    appelation_org = idmapis[f"groupappellation.{organization['id']}"]
    # connect Group Proxy and person in named graphbgn:BioDes
    g.add((node_org, RDF.type, crm.E74_Group))
    g.add((node_org, RDF.type, idmcore.Group))
    # defines group class
    g.add((node_org, owl.sameAs, URIRef(f"{base_uri}/entity/{organization['id']}")))
    for uri in organization["sameAs"]:
        g.add((node_org, owl.sameAs, URIRef(uri)))
    # defines group as the same group in the APIS dataset
    g.add((node_org, crm.P1_is_identified_by, appelation_org))
    g.add((appelation_org, rdfs.label, Literal(organization["name"], lang="de")))
    g.add((appelation_org, RDF.type, crm.E33_E41_Linguistic_Appellation))
    # add group appellation and define it as linguistic appellation
    if organization["start_date_written"] is not None:
        if len(organization["start_date_written"]) >= 4:
            start_date_node = idmapis[f"groupstart.{organization['id']}"]
            start_date_time_span = idmapis[f"groupstart.timespan.{organization['id']}"]
            # print(row['institution_name'], ':', row['institution_start_date'], row['institution_end_date'], row['institution_start_date_written'], row['institution_end_date_written'])
            g.add((start_date_node, RDF.type, crm.E63_Beginning_of_Existence))
            g.add((start_date_node, crm.P92_brought_into_existence, node_org))
            if organization["start_date"] is not None:
                g.add(
                    (
                        start_date_node,
                        URIRef(crm + "P4_has_time-span"),
                        start_date_time_span,
                    )
                )
                create_time_span_tripels("start", start_date_time_span, organization)
            # if len(res['start_date_written']) == 4 and res['start_end_date'] is not None:
            #     # check whether only a year has bin given for the start date and add according nodes
            #     g.add((start_date_time_span, crm.P82a_begin_of_the_begin, (Literal(
            #         f"{res['start_start_date']}T00:00:00", datatype=XSD.dateTime))))
            #     g.add((start_date_time_span, crm.P82b_end_of_the_end, (Literal(
            #         f"{res['start_end_date']}T23:59:59", datatype=XSD.dateTime))))
            # else:
            #     g.add((start_date_time_span, crm.P82a_begin_of_the_begin, (Literal(
            #         f"{res['start_date']}T00:00:00", datatype=XSD.dateTime))))
            #     g.add((start_date_time_span, crm.P82b_end_of_the_end, (Literal(
            #         f"{res['start_date']}T23:59:59", datatype=XSD.dateTime))))
    if organization["end_date_written"] is not None:
        if len(organization["end_date_written"]) >= 4:
            end_date_node = idmapis[f"groupend.{organization['id']}"]
            end_date_time_span = idmapis[f"groupend.timespan.{organization['id']}"]
            # print(row['institution_name'], ':', row['institution_start_date'], row['institution_end_date'], row['institution_start_date_written'], row['institution_end_date_written'])
            g.add((end_date_node, RDF.type, crm.E64_End_of_Existence))
            g.add((end_date_node, crm.P93_took_out_of_existence, node_org))
            if organization["end_date"] is not None:
                g.add(
                    (
                        end_date_node,
                        URIRef(crm + "P4_has_time-span"),
                        end_date_time_span,
                    )
                )
                create_time_span_tripels("end", end_date_time_span, organization)
            # if len(res['end_date_written']) == 4 and res['end_end_date'] is not None:
            #     # check whether only a year has bin given for the start date and add according nodes
            #     g.add((end_date_time_span, crm.P82a_begin_of_the_begin, (Literal(
            #         f"{res['end_start_date']}T00:00:00", datatype=XSD.dateTime))))
            #     g.add((end_date_time_span, crm.P82b_end_of_the_end, (Literal(
            #         f"{res['end_end_date']}T23:59:59", datatype=XSD.dateTime))))
            # else:
            #     g.add((end_date_time_span, crm.P82a_begin_of_the_begin, (Literal(
            #         f"{res['end_date']}T00:00:00", datatype=XSD.dateTime))))
            #     g.add((end_date_time_span, crm.P82b_end_of_the_end, (Literal(
            #         f"{res['end_date']}T23:59:59", datatype=XSD.dateTime))))
    return True


def render_event(event, event_type, node_event):
    """renders event object as RDF graph

    Args:
        event (_type_): _description_
        g (_type_): _description_
    """
    # prepare basic node types
    # node_event = URIRef(f"{idmapis}{event_type}/{event['id']}")
    global g
    node_event_role = idmapis[f"{event_type}.eventrole.{event['id']}"]
    node_pers = idmapis[f"personproxy.{event['related_person']['id']}"]
    node_roletype = idmrole[f"{event['relation_type']['id']}"]
    g.add((node_event_role, bioc.inheres_in, node_pers))
    g.add((node_event_role, RDF.type, node_roletype))
    g.add((node_roletype, rdfs.subClassOf, bioc.Event_Role))
    g.add(
        (node_roletype, RDFS.label, Literal(event["relation_type"]["label"], lang="de"))
    )
    # suggestion to add specific event role
    g.add((node_event, bioc.had_participant_in_role, node_event_role))
    # connect event and event role
    g.add((node_event, RDF.type, crm.E5_Event))
    # define crm classification
    g.add(
        (
            node_event_role,
            RDFS.label,
            Literal(event["relation_type"]["label"], lang="de"),
        )
    )
    g.add(
        (
            node_event,
            RDFS.label,
            Literal(
                f"{event['related_person']['label']} {event['relation_type']['label']} {event['related_place']['label']}",
                lang="de",
            ),
        )
    )
    if event["start_date"] is not None:
        node_timespan = idmapis[f"{event_type}.timespan.{event['id']}"]
        g.add((node_event, URIRef(crm + "P4_has_time-span"), node_timespan))
        # add time-span to event
        create_time_span_tripels("start", node_timespan, event)
        # add end of time-span
        if event["end_date"] is not None:
            create_time_span_tripels("end", node_timespan, event)
    return True


@task(tags=["render-rdflib"], timeout_seconds=60)
def render_place(place):
    """renders place object as RDF graph

    Args:
        place (_type_): _description_
        g (_type_): _description_
    """
    #    res = await get_entity(place, 'place')
    # setup basic nodes
    global g
    if place is None:
        return True
    node_place = idmapis[f"place.{place['id']}"]
    g.add((node_place, RDFS.label, Literal(place["name"], lang="de")))
    node_appelation = idmapis[f"placeappellation.{place['id']}"]
    node_plc_identifier = idmapis[f"placeidentifier.{place['id']}"]

    g.add((node_place, RDF.type, crm.E53_Place))
    # define place as Cidoc E53 Place
    g.add((node_place, crm.P1_is_identified_by, node_appelation))
    # add appellation to place
    g.add((node_appelation, RDF.type, crm.E33_E41_Linguistic_Appellation))
    # define appellation as linguistic appellation
    g.add((node_appelation, RDFS.label, Literal(place["name"], lang="de")))
    # add label to appellation
    g.add((node_place, owl.sameAs, URIRef(f"{base_uri}/entity/{place['id']}")))
    for uri in place["sameAs"]:
        g.add((node_place, owl.sameAs, URIRef(uri)))
    g.add(
        (
            node_place,
            crm.P1_is_identified_by,
            idmapis[f"placeidentifier.{place['id']}"],
        )
    )
    # add APIS Identifier as Identifier
    g.add((node_plc_identifier, RDF.type, crm.E_42_Identifier))
    # define APIS Identifier as E42 Identifier (could add a class APIS-Identifier or model a Identifier Assignment Event)
    g.add((node_plc_identifier, RDFS.label, Literal(place["id"])))
    # add label to APIS Identifier
    # define that individual in APIS named graph and APIS entity are the same
    if place["lat"] is not None and place["lng"] is not None:
        node_spaceprimitive = idmapis[f"spaceprimitive.{place['id']}"]
        g.add((node_place, crm.P168_place_is_defined_by, node_spaceprimitive))
        g.add((node_spaceprimitive, rdf.type, crm.E94_Space_Primitive))
        g.add(
            (
                node_spaceprimitive,
                crm.P168_place_is_defined_by,
                Literal(
                    (
                        f"Point ( {'+' if place['lng'] > 0 else ''}{place['lng']} {'+' if place['lat'] > 0 else ''}{place['lat']} )"
                    ),
                    datatype=geo.wktLiteral,
                ),
            )
        )
        # define that individual in APIS named graph and APIS entity are the same
    # suggestion for serialization of space primitives according to ISO 6709, to be discussed
    # more place details will be added (references, source, place start date, place end date, relations to other places(?))
    return True


class EntityTypeEnum(enum.Enum):
    person = "person"
    institution = "institution"
    place = "place"
    work = "work"
    event = "event"


class APIEndpointsEnum(enum.Enum):
    relations = "relations"
    entities = "entities"


@task(retries=3, retry_delay_seconds=30, timeout_seconds=120)
def request_from_api(
    url: HttpUrl | None = None,
    base_url: HttpUrl = "https://apis.acdh.oeaw.ac.at",
    filter_params: dict | None = None,
    entity_type: EntityTypeEnum | None = None,
    api_endpoint: APIEndpointsEnum = "entities",
    offset: int = None,
    mapped_id: int | None = None,
    mapped_filter_key: str | None = None,
    return_results_only: bool = False,
    return_count_only: bool = False,
):
    rate_limit("api-requests-apis")
    logger = get_run_logger()
    if url is not None:
        logger.info(f"Getting entities from {url}")
        res = requests.get(url)
    else:
        url = f"{base_url}/apis/api/{api_endpoint}/{entity_type}"
        logger.info(f"Getting entities from {url}")
        if filter_params is None:
            filter_params = {"format": "json"}
        if offset is not None and "offset" not in filter_params:
            filter_params["offset"] = offset
        if "format" not in filter_params:
            filter_params["format"] = "json"
        if mapped_filter_key is not None:
            filter_params[mapped_filter_key] = mapped_id
        res = requests.get(url, params=filter_params)
    if res.status_code == 200:
        if return_count_only:
            return res.json()["count"]
        elif return_results_only:
            return res.json()["results"]
        else:
            return res.json()
    else:
        logger.error(f"Error getting entities from {url}")
        logger.error(res.status_code)
        logger.error(res.text)
        raise Exception(f"Error getting entities from {url}")


@task()
def create_base_graph(base_uri, named_graph):
    global crm
    crm = Namespace("http://www.cidoc-crm.org/cidoc-crm/")
    """Defines namespace for CIDOC CRM."""
    global ex
    ex = Namespace("http://www.intavia.eu/")
    """Defines namespace for own ontology."""
    global idmcore
    idmcore = Namespace("http://www.intavia.eu/idm-core/")
    """Defines namespace for own ontology."""
    global idmrole
    idmrole = Namespace("http://www.intavia.eu/idm-role/")
    """Defines namespace for role ontology."""
    global idmapis
    idmapis = Namespace("http://www.intavia.eu/apis/")
    """Namespace for InTaVia named graph"""
    global idmbibl
    idmbibl = Namespace("http://www.intavia.eu/idm-bibl/")
    """Namespace for bibliographic named graph"""
    global idmrelations
    idmrelations = Namespace("http://www.intavia.eu/idm-relations")
    """Defines namespace for relation ontology."""
    global intavia_shared
    intavia_shared = Namespace("http://www.intavia.eu/shared-entities")
    """Defines namespace for relation ontology."""
    global ore
    ore = Namespace("http://www.openarchives.org/ore/terms/")
    """Defines namespace for schema.org vocabulary."""
    global edm
    edm = Namespace("http://www.europeana.eu/schemas/edm/")
    """Defines namespace for Europeana data model vocabulary."""
    global owl
    owl = Namespace("http://www.w3.org/2002/07/owl#")
    """Defines namespace for Europeana data model vocabulary."""
    global rdf
    rdf = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
    """Defines namespace for Europeana data model vocabulary."""
    global xml
    xml = Namespace("http://www.w3.org/XML/1998/namespace")
    """Defines namespace for Europeana data model vocabulary."""
    global xsd
    xsd = Namespace("http://www.w3.org/2001/XMLSchema#")
    """Defines namespace for Europeana data model vocabulary."""
    global bioc
    bioc = Namespace("http://ldf.fi/schema/bioc/")
    """Defines namespace for Europeana data model vocabulary."""
    global rdfs
    rdfs = Namespace("http://www.w3.org/2000/01/rdf-schema#")
    """Defines namespace for Europeana data model vocabulary."""
    global apis
    apis = Namespace(base_uri)
    """Defines namespace for APIS database."""
    global bf
    bf = Namespace("http://id.loc.gov/ontologies/bibframe/")
    """Defines bibframe namespace."""
    global geo
    geo = Namespace("http://www.opengis.net/ont/geosparql#")
    global g
    combined_graph = Dataset()
    context = URIRef(named_graph)
    g = combined_graph.graph(context)
    g.bind("apis", apis)
    g.bind("crm", crm)
    g.bind("intaviashared", intavia_shared)
    g.bind("ore", ore)
    g.bind("edm", edm)
    g.bind("owl", owl)
    g.bind("rdf", rdf)
    g.bind("xml", xml)
    g.bind("xsd", xsd)
    g.bind("bioc", bioc)
    g.bind("rdfs", rdfs)
    g.bind("apis", apis)
    g.bind("idmcore", idmcore)
    g.bind("idmrole", idmrole)
    g.bind("idmrelations", idmrelations)
    g.bind("owl", owl)
    g.bind("geo", geo)
    g.bind("bf", bf)
    g.bind("ex", ex)
    g.bind("idmbibl", idmbibl)
    g.bind("idmapis", idmapis)
    global glob_list_entities
    glob_list_entities = {"institution": [], "person": [], "place": []}
    global glob_list_vocabs
    glob_list_vocabs = dict()
    return True


@task(tags=["rdf", "serialization"])
def serialize_graph(g, storage_path, add_date_to_file):
    """serializes the RDFLib graph to a given destination

    Args:
        g (rflib): the graph to serialize
        storage_path (str): path within the container to store the file to
        named_graph (uri): optional named graph to serialize

    Returns:
        path: path of the stored file
    """

    Path(storage_path).mkdir(parents=True, exist_ok=True)
    for s, p, o in g.triples((None, bioc.inheres_in, None)):
        g.add((o, bioc.bearer_of, s))
    Path(storage_path).mkdir(parents=True, exist_ok=True)
    if add_date_to_file:
        new_name = f"{storage_path.stem}_{datetime.now().strftime('%d-%m-%Y')}.nq"
        storage_path = storage_path.with_name(new_name)
    else:
        # Just add the extension
        storage_path = storage_path.with_suffix(".ttl")
    g.serialize(destination=storage_path, format="ttl")
    return storage_path


class Params(BaseModel):
    max_entities: str = Field(
        None,
        description="Number of entities to retrieve (int). Is just limiting the API requests, mainly used for test purposes",
    )
    named_graph: HttpUrl = Field(
        "http://data.acdh.oeaw.ac.at/apis",
        description="Named graph used to store the data in. Not used if graph is submitted to PR",
    )
    endpoint: HttpUrl = Field(
        "https://apis.acdh.oeaw.ac.at",
        description="Base APIS installation URL to use for sarialization",
    )
    base_uri_serialization: HttpUrl = Field(
        "https://apis.acdh.oeaw.ac.at",
        description="Base URL used to create URIs in the RDF",
    )
    filter_params: dict = Field(
        {"collection": 86},
        description="Filters applied on the first call to the persons API. Default searches for persons depicted in the ÖBL",
    )
    storage_path: DirectoryPath = Field(
        "/archive/serializations/APIS",
        description="Path of the turtle file to use for serialization within the container excuting the flow",
    )
    add_date_to_file: bool = Field(
        False,
        alias="Add date to file",
        description="Whether to add the current date to the file name",
    )
    branch: str = Field(..., description="GIT branch to push to and create PR")
    upload_data: bool = Field(
        False,
        description="Whether to directly upload the data into a configured triplestore",
    )
    push_data_to_repo: bool = Field(
        True,
        description="Whether to push the data to a configured repo and create a PR.",
    )
    page_size: int = Field(
        100, description="Page size used for requests against the API"
    )


@flow
def gather_render_tasks(data, render_task, **kwargs):
    res = []
    for item in data:
        # Convert any additional kwargs to unmapped parameters
        unmapped_kwargs = {k: unmapped(v) for k, v in kwargs.items()}
        res.extend(render_task.map(item, **unmapped_kwargs))
    return res


@task
def fetch_person_relations(person_ids, relation_type, filter_key):
    """Fetch person relations in batches to reduce parallel jobs"""
    batch_size = 50  # Process 50 persons at a time
    results = []

    for i in range(0, len(person_ids), batch_size):
        batch_ids = person_ids[i : i + batch_size]
        batch_results = request_from_api(
            mapped_id=batch_ids,
            mapped_filter_key=filter_key,
            api_endpoint="relations",
            entity_type=relation_type,
            return_results_only=True,
        )
        results.extend(batch_results)

    return results


@flow
def create_apis_rdf_serialization_v3(params: Params):
    logger = get_run_logger()
    create_base_graph(params.base_uri_serialization, params.named_graph)
    global base_uri
    base_uri = params.endpoint

    # Step 1: Get initial person list
    logger.info("Fetching initial person list")
    pers_counts = request_from_api(
        filter_params=params.filter_params,
        return_count_only=True,
        entity_type="person",
    )

    # Fetch persons in smaller batches
    batch_size = min(500, pers_counts)  # Limit batch size
    pers_list_initial = []
    for offset in range(0, pers_counts, batch_size):
        batch = request_from_api(
            offset=offset,
            filter_params=params.filter_params,
            return_results_only=True,
            entity_type="person",
        )
        pers_list_initial.extend(batch)

    pers_list_initial_ids = list(set([item["id"] for item in pers_list_initial]))

    # Step 2: Render initial person list
    logger.info("Rendering person data")
    pers_list_initial_render = gather_render_tasks([pers_list_initial], render_person)

    # Step 3: Fetch and render relations in sequence
    logger.info("Processing person-place relations")
    pers_place_relations = fetch_person_relations(
        pers_list_initial_ids, "personplace", "related_person"
    )
    pers_place_relations_render = gather_render_tasks(
        [pers_place_relations], render_personplace_relation
    )

    logger.info("Processing person-institution relations")
    pers_inst_relations = fetch_person_relations(
        pers_list_initial_ids, "personinstitution", "related_person"
    )
    pers_inst_relations_render = gather_render_tasks(
        [pers_inst_relations], render_personinstitution_relation
    )
    pers_inst_relations_vocab_render = gather_render_tasks(
        [pers_inst_relations], render_personrole_from_relation
    )

    logger.info("Processing person-person relations")
    pers_pers_relationsA = fetch_person_relations(
        pers_list_initial_ids, "personperson", "related_personA"
    )
    pers_pers_relationsB = fetch_person_relations(
        pers_list_initial_ids, "personperson", "related_personB"
    )
    pers_pers_relations_render = gather_render_tasks(
        [pers_pers_relationsA], render_personperson_relation
    )
    pers_pers_relations_renderB = gather_render_tasks(
        [pers_pers_relationsB], render_personperson_relation
    )

    # Step 4: Process additional entities
    logger.info("Processing additional places and institutions")
    if glob_list_entities["place"]:
        additional_places = request_from_api(
            mapped_id=glob_list_entities["place"],
            mapped_filter_key="id",
            entity_type="place",
            return_results_only=True,
        )
        additional_places_render = gather_render_tasks(
            [additional_places], render_place
        )

    if glob_list_entities["institution"]:
        additional_institutions = request_from_api(
            mapped_id=glob_list_entities["institution"],
            mapped_filter_key="id",
            entity_type="institution",
            return_results_only=True,
        )
        additional_institutions_render = gather_render_tasks(
            [additional_institutions], render_organization
        )

    # Step 5: Serialize the graph
    logger.info("Serializing graph")
    file_path = serialize_graph(
        g,
        params.storage_path,
        params.add_date_to_file,
    )
    #    if params.push_data_to_repo:
    #        logger.info(f"Pushing data to repo, using file path: {file_path.result()}")
    #        push_data_to_repo_flow(
    #            params=ParamsPush(branch_name=params.branch, file_path=file_path.result())
    #        )
    if params.upload_data:
        upload_data(file_path, params.named_graph, wait_for=[file_path])
    print("pers_place_relations_render", pers_place_relations_render)


if __name__ == "__main__":
    create_apis_rdf_serialization_v3(
        params=Params(
            filter_params={"first_name": "Johann"},
            storage_path="/home/sennierer/projects/prosnet-prefect-pipelines/testdata/",
            branch="test",
        )
    )
