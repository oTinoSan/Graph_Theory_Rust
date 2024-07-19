use super::csr::Csr;

pub fn shiloach_vishkin(graph: Csr) -> Vec<usize> {
    let mut component: Vec<usize> = vec![];

    for i in 0..graph.rof_offset.len() - 1{
        component.push(i); 
    }

    let mut graft: bool = true;
    let mut iter = 0;

    while graft {
        graft = false; 
        iter +=1;

        for index in 0..graph.rof_offset.len() - 1{
            for k in graph.rof_offset[index].. graph.rof_offset[index + 1]{
                let row = index;
                let col = graph.col_indices[k as usize] as usize;

                if component[row] < component[col] && component[col] == component[component[col]] {
                    let temp = component[col];
                    component[temp] = component[row];
                    graft = true;
                } 

                if component[col] < component[row] && component[row] == component[component[row]] {
                    let temp = component[row];
                    component[temp] = component[col];
                    graft = true;
                }
            }
        }

        for index in 0..graph.rof_offset.len() - 1{
            while component[index] != component[component[index]]{
                component[index] = component[component[index]];
            }
        }

    }

    component
}